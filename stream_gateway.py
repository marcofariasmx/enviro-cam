#!/usr/bin/env python3
"""
On-demand live streaming for rancho-cam-pi.

Runs inside the SAME process (and same open Picamera2 instance) as
sender.py's periodic still-capture loop -- Picamera2/libcamera only allows
one process to hold the camera at a time (the "Device or resource busy"
lesson, same one already learned with the Growatt inverter's Modbus port),
so this is wired in as a background thread of sender.py's daemon rather
than a separate service. See stream_feasibility findings: the still
capture continues completely unaffected while this module drives a
concurrent encoder on the same camera.

Transport is raw H.264 over a long-lived HTTP GET, broadcast to however
many clients are connected (in practice just main-pi5, which packages it
into HLS for browsers -- see the monitor-cam-webapp side).

Codec history, because this has moved twice and the reasons matter:
  1. H.264 -> ffmpeg -> fragmented MP4 -> MediaSource/appendBuffer in the
     browser. The transport was fine; the CLIENT side was not -- Chrome's
     CHUNK_DEMUXER_ERROR_APPEND_FAILED class of failures throws nothing and
     logs nothing, so it presented only as "connects once, then never
     again." Abandoned.
  2. MJPEG over multipart/x-mixed-replace, rendered by a plain <img>. Dead
     simple and genuinely reliable -- but MJPEG is intra-only: every frame
     is a whole JPEG, so a near-static landscape re-sends ~99% identical
     pixels forever. On the ranch's ~1-2 Mbps uplink that capped us at
     640x480, and it still looked soft. Compression was no longer the
     limit; resolution was.
  3. (now) H.264 again -- but delivered as HLS, packaged by ffmpeg on
     main-pi5 and played by hls.js/native Safari. H.264's inter-frame
     prediction is ~10-20x more efficient than MJPEG on a static scene,
     which is what buys 1280x720 inside the same budget. The lesson from
     (1) was never "H.264 is bad", it was "don't hand-roll the browser-side
     media pipeline" -- hls.js is a battle-tested library, not our code.

Encoding picks between the camera's existing "main" (1280x720) and "lores"
(640x480) streams based on the budget -- see HD_MIN_BITRATE_KBPS. Both are
already configured by sender.py, so this never reconfigures the camera,
which keeps the 5-minute still capture (and its 15s night exposure) fully
out of the blast radius of any streaming change.

Bitrate and the hard session-duration cap are both supplied by the caller
(main-pi5 decides bitrate from real uplink headroom); this module just
enforces sane bounds and never trusts the caller's numbers blindly.

Session history (bytes actually pushed, duration, how it ended) is logged
locally via stream_storage.py and exported as Prometheus textfile counters
-- same durability rationale as growatt_storage.py: don't let this data
depend on main-pi5's Prometheus successfully scraping at the right moment.
"""
import io
import json
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from picamera2.encoders import H264Encoder
from picamera2.outputs import FileOutput

import stream_storage

logger = logging.getLogger(__name__)

PORT = 8091
DB_PATH = "/home/mafx/enviro-cam/stream_sessions.db"
PROM_PATH = "/var/lib/prometheus/node-exporter/stream.prom"

# Hard bounds enforced regardless of what the caller requests -- these
# protect the ranch's uplink and the camera's encoder from a bad/malicious
# request, independent of whatever bandwidth-gating logic main-pi5 applies
# on its side.
MIN_BITRATE_KBPS = 150
MAX_BITRATE_KBPS = 2000
MAX_SESSION_SECONDS = 20 * 60  # 20 min hard cap per session

# The Pi's V4L2 H.264 encoder substantially UNDERSHOOTS its bitrate target
# on this content -- measured end to end:
#   asked 200 kbps -> 83 actual (41%)
#   asked 600 kbps -> 282 actual (47%)
#   asked 1500 kbps -> 385 actual (26%, clearly saturating)
# Passing the network budget through unscaled therefore spends only ~40% of
# what the link can afford, and the encoder pays for that by crushing the
# P-frames: measured 200-900 byte P-frames against a ~7 KB I-frame, a 10x
# ratio. That is what produced the visible ~2s "heartbeat" -- each keyframe
# arrives comparatively sharp and the starved frames after it visibly decay
# until the next one resets the cycle. Scaling the request up fills the
# budget and evens the frames out.
# Deliberately conservative (the real ratio is 41-47% in the range we
# actually operate in): overestimating the efficiency lands us UNDER budget,
# the safe direction on a link the rest of the ranch shares.
H264_BITRATE_EFFICIENCY = 0.45
MAX_ENCODER_BITRATE_KBPS = 6000

# Constant-quantiser (qp) mode was tried and REMOVED. It equalises the
# quantiser across I and P frames, but the keyframe pulse comes from
# P-frames accumulating refinement, not from unequal quantisation --
# measured ~13.6% drop versus ~9% for CBR, i.e. worse. It also discards the
# bitrate ceiling that protects the ranch's uplink. Same verdict for a QP
# *range*: `12-40` produced a 0 kbps stream and `8-45` produced 1062 kbps
# against a 400 kbps budget. Neither is safe to expose. See homelab-map.

# Which camera stream to encode is chosen per session from the budget.
# Both already exist in sender.py's configuration, so switching between
# them costs nothing and -- crucially -- needs no camera reconfiguration,
# which would put the 5-minute still capture at risk.
#
# 1280x720 needs real bitrate to be worth it. Measured on a 192 kbps
# budget (the ranch's uplink had fallen to 0.6 Mbps), 720p came back
# visibly smeared -- worse to look at than a clean lower resolution would
# be. Below the threshold, encode the 640x480 stream instead and spend the
# whole budget on making *it* clean.
HD_STREAM_NAME = "main"    # 1280x720
SD_STREAM_NAME = "lores"   # 640x480
HD_MIN_BITRATE_KBPS = 500

# Frame rate no longer has to be traded against image quality: H.264's
# inter-frame prediction makes additional frames of a near-static scene
# almost free, which is the entire reason this moved off MJPEG. The cap
# exists only because the camera's video config allows a 100us minimum
# frame duration -- measured ~113 fps on real hardware -- and nobody needs
# 113 fps of a landscape.
STREAM_FPS = 12
# Frame rate used when the budget is thin, alongside the drop to 640x480.
#
# This is about the KEYFRAME PULSE, not about bandwidth. Measured in
# daylight at a 400 kbps budget, the quality drop at each keyframe was:
#     12 fps (24-frame GOP) -> 14.4%
#      6 fps (12-frame GOP) ->  6.7%
#      3 fps  (6-frame GOP) ->  3.7%
# Note the bits PER FRAME were identical in all three (~30 kbit; this
# encoder holds a fixed ~0.096 bits/pixel quality ceiling and simply emits
# less data at lower frame rates -- more bitrate does not raise it, which
# is the same saturation seen when asking 1500 kbps and getting 385).
# So the pulse does not shrink because frames get more bits. It shrinks
# because a shorter GOP accumulates less P-frame refinement, leaving less
# for the next keyframe to throw away. Fewer frames per GOP = smaller step.
# At a healthy budget the pulse measured 0.0% at 12 fps, so this only
# applies when the link is already forcing us down to 640x480.
STREAM_FPS_LOW_BUDGET = 6

# Auto-white-balance is LOCKED for the duration of a streaming session.
#
# Measured on real hardware at night (Lux 0.12, gain pinned at its 16.0
# ceiling): exposure and gain were rock steady -- ExposureTime sat at
# exactly 66655us sample after sample -- but the colour gains never
# settled, swinging R 1.62-1.70 and B 2.38-2.51, i.e. a colour temperature
# wandering over a ~260K range (3589-3851K) continuously. In a dark scene
# the eye is very sensitive to that, and it reads as the picture endlessly
# "recalibrating black" -- an on/off tone flicker. It is chromatic, not
# luminance: nothing to do with exposure.
#
# Freezing AWB at whatever the scene actually metered when the session
# started keeps the colour correct while making it STOP moving. Sessions
# are capped at 10 minutes, so there is no meaningful window for real
# lighting to drift away from the locked value. Restored on stop so the
# 5-minute still capture keeps full auto white balance.
AWB_SETTLE_READS = 3
# ...but bounded in wall clock too, not just in frames. capture_metadata()
# blocks until the next frame COMPLETES, and a frame is only 1/fps long
# once the camera is actually running at streaming speed. Coming out of a
# night still capture a frame can be 15 seconds, so three reads is 45
# seconds of a caller that main-pi5 gives up on after 10.
AWB_SETTLE_TIMEOUT_S = 3.0
# Seconds between I-frames. This is a direct quality/latency trade and it
# matters far more than it looks on a link this thin: an I-frame is coded
# from scratch, and a 720p one can cost more bits than an entire second of
# a 192 kbps budget -- so keyframing every second forced rate control to
# starve everything in between, which is exactly the smearing that showed
# up in testing. Every 2s halves that overhead. It also sets the floor on
# HLS segment duration (segments can only be cut on an I-frame), and hence
# on latency, so it should not grow without reason.
STREAM_IFRAME_SECONDS = 2
# Restored on stop -- must match sender.py's init_camera() configuration.
# The 15.1s maximum is what lets the night-mode still capture take its 15s
# manual exposure, so putting it back is not optional; a session that ended
# without restoring it would cap the next night's stills at
# STREAM_MAX_FRAME_DURATION_US and quietly ruin them.
IDLE_MIN_FRAME_DURATION_US = 100
MAX_FRAME_DURATION_US = 15_100_000
# The frame-duration ceiling while a session is live -- a floor of 5fps on
# the picture actually reaching the viewer.
#
# The streaming path used to pass MAX_FRAME_DURATION_US here, which is
# correct for the still capture and useless for video: in darkness
# auto-exposure opens all the way up, so the "12fps" stream produced a new
# frame every several seconds. Measured on a real night session: 43 KB sent
# in 13 seconds against a 1200 kbps budget, i.e. a still picture. A live
# view is worth more grainy than frozen, so bound the exposure here and let
# gain carry the rest of the darkness.
STREAM_MAX_FRAME_DURATION_US = 200_000

# The camera has exactly one owner at a time.
#
# One Picamera2 instance is shared by two writers: this gateway, and the
# 5-minute still capture over in sender.py. Nothing mutually excluded them,
# and at night -- where the still capture takes MANUAL control for up to two
# minutes of every five while its exposure ramp climbs to 15s -- that broke
# both directions at once:
#
#   - start_stream()'s AWB settle blocked for three 15s frames, far past
#     main-pi5's 10s HTTP timeout, so the viewer got a bare 502.
#   - Any session that did start inherited those multi-second frames.
#   - And the stream stole frames back from the ramp's _await_exposure(),
#     which then gave up and metered a stale one: "attempt 3 @ 8.08s ->
#     brightness 48", the identical reading to attempt 2 @ 3.00s.
#
# Whoever holds this owns the camera's controls. The still capture is the
# priority -- it is the archive, the stream is best-effort -- so it preempts
# a live session rather than queueing behind one.
camera_lock = threading.RLock()
# How long start_stream() waits on that lock before reporting the camera
# busy. Deliberately well under the relay's HTTP timeout: a prompt "busy,
# try again" is worth much more to the browser than a timeout.
CAMERA_BUSY_WAIT_S = 4.0
# What we tell the caller to wait. A night still capture runs ~2 minutes,
# but it is nearly always partway through by the time anyone asks, and
# retrying costs one cheap request -- so retry often rather than making a
# viewer who arrived at the tail end of one sit out the whole window.
CAMERA_BUSY_RETRY_S = 10

# A camera that has just taken a 15s night still cannot stream yet, and
# there is no control that makes it able to.
#
# Coming off a multi-second cadence costs the frames already in the
# pipeline, and they run at the OLD duration: measured 73 seconds to get
# back to a short exposure after a 15s still, with the streaming
# frame-duration ceiling applied the whole time and ignored throughout.
# Handing exposure back to auto does not help, nor does driving it back
# manually -- both were tried and measured. It is queue latency, not a
# setting.
#
# So a session started inside that window does not stream badly, it streams
# nothing: 0 bytes over 12 seconds, 28 KB over 90. Refusing to start is
# strictly better than starting a dead one -- the caller retries, and gets
# a session that actually carries picture.
#
# Estimated rather than measured because measuring means capture_metadata(),
# which blocks for a whole frame and would itself take 15 seconds here.
RECOVERY_FRAMES = 6
MAX_RECOVERY_S = 120.0

_camera_ready_at = 0.0  # time.monotonic()
_ready_lock = threading.Lock()


def note_long_exposure(exposure_us):
    """Told by the still capture what exposure it just used, so streaming
    can hold off until the sensor is off that cadence. See RECOVERY_FRAMES."""
    global _camera_ready_at
    recovery_s = min(MAX_RECOVERY_S, RECOVERY_FRAMES * exposure_us / 1_000_000)
    with _ready_lock:
        _camera_ready_at = time.monotonic() + recovery_s
    logger.info(
        f"Camera coming off a {exposure_us / 1_000_000:.1f}s exposure -- "
        f"holding streaming off for {recovery_s:.0f}s"
    )


def _camera_recovery_remaining_s():
    with _ready_lock:
        return max(0.0, _camera_ready_at - time.monotonic())

_CLIENT_QUEUE_MAXSIZE = 300  # ~a few seconds of buffered H.264 at these bitrates


class _Broadcaster(io.BufferedIOBase):
    """Fans out encoder output to every currently-connected HTTP client.
    A slow/stalled client gets its oldest buffered bytes dropped rather
    than blocking (or slowing down) the encoder thread for everyone else.

    Subclasses io.BufferedIOBase because picamera2's FileOutput requires
    it (raises "Must pass io.BufferedIOBase" otherwise) -- confirmed live
    the hard way when a plain object here silently killed every stream
    start attempt.

    Raw H.264 passes through untouched: the only consumer is main-pi5's
    ffmpeg, which parses the Annex-B byte stream itself and needs no
    framing added here. (The MJPEG version this replaced wrapped each frame
    in a multipart part; H.264 has no such per-frame boundary to add.)
    """

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._clients = []
        self.bytes_written = 0

    def writable(self):
        return True

    def register(self):
        q = queue.Queue(maxsize=_CLIENT_QUEUE_MAXSIZE)
        with self._lock:
            self._clients.append(q)
        return q

    def unregister(self, q):
        with self._lock:
            if q in self._clients:
                self._clients.remove(q)

    def write(self, buf):
        data = bytes(buf)
        with self._lock:
            clients = list(self._clients)
            self.bytes_written += len(data)
        for q in clients:
            try:
                q.put_nowait(data)
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait(data)
                except queue.Empty:
                    pass
        return len(data)

    def flush(self):
        pass


class _State:
    """All mutable session state, behind one lock."""

    def __init__(self):
        self.lock = threading.Lock()
        self.active = False
        self.session_id = None
        self.started_monotonic = None
        self.started_at_iso = None
        self.bitrate_kbps = None
        self.deadline_timer = None
        self.broadcaster = None


_state = _State()
_picam2 = None


def _write_prom_textfile():
    totals = stream_storage.get_totals(DB_PATH)
    last = stream_storage.get_last_session(DB_PATH)
    lines = [
        f'homelab_stream_sessions_total {totals["session_count"]}',
        f'homelab_stream_bytes_total {int(totals["total_bytes"])}',
        f'homelab_stream_seconds_total {totals["total_duration_s"]:.1f}',
    ]
    if last:
        lines.append(f'homelab_stream_last_duration_seconds {last["duration_s"] or 0:.1f}')
        lines.append(f'homelab_stream_last_bytes {last["bytes_sent"] or 0}')
    tmp = PROM_PATH + ".tmp"
    try:
        with open(tmp, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(tmp, PROM_PATH)
        os.chmod(PROM_PATH, 0o644)
    except OSError as e:
        logger.warning(f"Could not write {PROM_PATH}: {e}")


def _stop_locked(reason):
    """Caller must hold _state.lock."""
    if not _state.active:
        return
    if _state.deadline_timer:
        _state.deadline_timer.cancel()
        _state.deadline_timer = None
    try:
        _picam2.stop_encoder()
    except Exception as e:
        logger.warning(f"stop_encoder error: {e}")
    try:
        # Hand white balance back to auto -- the 5-minute still capture
        # should keep adapting to real light.
        _picam2.set_controls({"AwbEnable": True})
    except Exception as e:
        logger.warning(f"AWB restore error: {e}")
    try:
        # Hand the camera back its unconstrained frame rate -- the still
        # capture path shares this same Picamera2 instance and shouldn't be
        # left running at streaming's reduced rate once nobody's watching.
        _picam2.set_controls({
            "FrameDurationLimits": (IDLE_MIN_FRAME_DURATION_US, MAX_FRAME_DURATION_US)
        })
    except Exception as e:
        logger.warning(f"FrameDurationLimits restore error: {e}")

    duration_s = time.monotonic() - _state.started_monotonic
    bytes_sent = _state.broadcaster.bytes_written
    stream_storage.end_session(
        DB_PATH, _state.session_id,
        datetime.now(timezone.utc).isoformat(), duration_s, bytes_sent, reason,
    )
    _write_prom_textfile()
    logger.info(
        f"Stream stopped ({reason}): {duration_s:.0f}s, "
        f"{bytes_sent / 1024:.0f} KB @ {_state.bitrate_kbps}kbps"
    )

    _state.active = False
    _state.session_id = None
    _state.broadcaster = None


def start_stream(bitrate_kbps, max_seconds, fps=None):
    """Idempotent: returns the current session info whether or not this
    call actually started a new one."""
    bitrate_kbps = max(MIN_BITRATE_KBPS, min(MAX_BITRATE_KBPS, int(bitrate_kbps)))
    max_seconds = max(10, min(MAX_SESSION_SECONDS, int(max_seconds)))

    # The still capture owns the camera while it runs -- see camera_lock.
    if not camera_lock.acquire(timeout=CAMERA_BUSY_WAIT_S):
        logger.info("Stream start refused: the 5-minute still capture holds the camera")
        return {"status": "camera_busy", "retry_after_s": CAMERA_BUSY_RETRY_S}
    try:
        # Checked while holding the lock: the capture publishes this in its
        # own teardown, so reading it any earlier can race the value.
        recovering_s = _camera_recovery_remaining_s()
        if recovering_s > 0:
            logger.info(
                f"Stream start held off: camera still coming off the still "
                f"capture's long exposure ({recovering_s:.0f}s left)"
            )
            return {"status": "camera_busy", "retry_after_s": int(recovering_s) + 2}
        return _start_stream_locked(bitrate_kbps, max_seconds, fps)
    finally:
        camera_lock.release()


def _start_stream_locked(bitrate_kbps, max_seconds, fps):
    """Caller must hold camera_lock."""
    with _state.lock:
        if _state.active:
            return {"status": "already_active", "bitrate_kbps": _state.bitrate_kbps}

        broadcaster = _Broadcaster()
        hd = bitrate_kbps >= HD_MIN_BITRATE_KBPS
        stream_name = HD_STREAM_NAME if hd else SD_STREAM_NAME
        if fps is None:
            fps = STREAM_FPS if hd else STREAM_FPS_LOW_BUDGET
        else:
            fps = max(1, min(30, int(fps)))
        # Cap the frame rate BEFORE starting the encoder so its rate control
        # is sized against the frame rate actually being produced.
        #
        # Belt and braces on exposure: zero means "auto decides", and this
        # clears any manual value still pinned on the shared camera.
        #
        # It is not sufficient on its own -- a sensor already running a
        # multi-second cadence stays on it regardless of what AE is told,
        # which is why the night capture flushes the camera back to a short
        # exposure before releasing it (see RELEASE_EXPOSURE_US in
        # sender.py). This covers the case where a capture died before it
        # got that far.
        _picam2.set_controls({
            "AeEnable": True,
            "ExposureTime": 0,
            "AnalogueGain": 0.0,
        })
        _picam2.set_controls({
            "FrameDurationLimits": (int(1_000_000 / fps), STREAM_MAX_FRAME_DURATION_US)
        })
        # iperiod = one I-frame every STREAM_IFRAME_SECONDS, and repeat=True
        # so SPS/PPS ride along with every one of them. Both matter downstream:
        #  - a consumer connecting mid-stream (main-pi5's ffmpeg always does)
        #    gets "non-existing PPS 0 referenced" and decodes nothing until
        #    the next IDR carrying headers. At the default ~5s I-frame
        #    period that's a 5 second black wait on every single start.
        #  - HLS segments can only be cut on an I-frame, so the I-frame
        #    period is the floor on segment duration -- and segment duration
        #    is what sets end-to-end latency.
        encoder_kbps = min(int(bitrate_kbps / H264_BITRATE_EFFICIENCY), MAX_ENCODER_BITRATE_KBPS)
        encoder = H264Encoder(
            bitrate=encoder_kbps * 1000,
            repeat=True,
            iperiod=fps * STREAM_IFRAME_SECONDS,
            framerate=fps,
        )
        _picam2.start_encoder(encoder, FileOutput(broadcaster), name=stream_name)

        # Lock AWB to what the scene metered just now -- see AWB_SETTLE_READS.
        try:
            gains = None
            deadline = time.monotonic() + AWB_SETTLE_TIMEOUT_S
            for _ in range(AWB_SETTLE_READS):
                if time.monotonic() >= deadline:
                    logger.info("AWB settle cut short by its time budget")
                    break
                md = _picam2.capture_metadata()
                if md.get("ColourGains"):
                    gains = md["ColourGains"]
            if gains:
                _picam2.set_controls({"AwbEnable": False, "ColourGains": tuple(gains)})
                logger.info(f"AWB locked at ColourGains {tuple(round(g, 3) for g in gains)}")
        except Exception as e:
            logger.warning(f"Could not lock AWB (stream continues, tone may drift): {e}")

        _state.active = True
        _state.started_monotonic = time.monotonic()
        _state.started_at_iso = datetime.now(timezone.utc).isoformat()
        _state.bitrate_kbps = bitrate_kbps
        _state.broadcaster = broadcaster
        _state.session_id = stream_storage.start_session(DB_PATH, _state.started_at_iso, bitrate_kbps)

        timer = threading.Timer(max_seconds, lambda: stop_stream("max_duration"))
        timer.daemon = True
        timer.start()
        _state.deadline_timer = timer

        logger.info(
            f"Stream started (H.264 {stream_name}) @ {bitrate_kbps}kbps budget "
            f"({encoder_kbps}kbps encoder), "
            f"{fps}fps, I-frame every {STREAM_IFRAME_SECONDS}s, "
            f"hard cap {max_seconds}s"
        )
        return {"status": "started", "bitrate_kbps": bitrate_kbps, "fps": fps}


def stop_stream(reason="requested"):
    with _state.lock:
        _stop_locked(reason)
        return {"status": "stopped"}


def stop_stream_for_capture():
    """Preempt any live session so the 5-minute still can take the camera.

    Called by sender.py while it holds camera_lock. Returns whether there
    was actually a session to tear down, so the caller can say so in the
    log. The archive wins over the live view; the browser reconnects on its
    own once the capture is done.
    """
    with _state.lock:
        was_active = _state.active
        _stop_locked("still_capture")
        return was_active


def status():
    with _state.lock:
        if not _state.active:
            return {"active": False}
        return {
            "active": True,
            "bitrate_kbps": _state.bitrate_kbps,
            "elapsed_s": round(time.monotonic() - _state.started_monotonic, 1),
        }


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logger.debug("%s - %s", self.address_string(), fmt % args)

    def _authed(self):
        return self.headers.get("X-Api-Key") == self.server.api_key

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/camera/metadata":
            # Diagnostic: what the camera is actually doing right now.
            # Added to investigate a night-time tone/brightness flicker --
            # sampling ExposureTime/AnalogueGain/ColourGains over time is
            # the only way to tell AE hunting apart from AWB hunting.
            if not self._authed():
                return self._json(401, {"error": "bad api key"})
            try:
                md = _picam2.capture_metadata()
            except Exception as e:
                return self._json(503, {"error": str(e)})
            keep = ("ExposureTime", "AnalogueGain", "DigitalGain", "ColourGains",
                    "Lux", "FrameDuration", "AeLocked", "ColourTemperature")
            return self._json(200, {k: md.get(k) for k in keep if k in md})

        if self.path == "/health":
            return self._json(200, {"status": "ok", **status()})

        if self.path == "/stream/raw.h264":
            if not self._authed():
                return self._json(401, {"error": "bad api key"})
            with _state.lock:
                if not _state.active:
                    return self._json(409, {"error": "not streaming"})
                broadcaster = _state.broadcaster
                q = broadcaster.register()
            self.send_response(200)
            self.send_header("Content-Type", "video/h264")
            self.end_headers()
            try:
                while True:
                    chunk = q.get(timeout=30)
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError, TimeoutError, queue.Empty):
                pass
            finally:
                broadcaster.unregister(q)
            return

        self._json(404, {"error": "not found"})

    def do_POST(self):
        if not self._authed():
            return self._json(401, {"error": "bad api key"})

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            body = {}

        if self.path == "/stream/start":
            bitrate_kbps = body.get("bitrate_kbps", MIN_BITRATE_KBPS)
            max_seconds = body.get("max_seconds", 300)
            # `fps` is an optional override kept for measurement; it is
            # bounded and cannot bypass the bitrate ceiling. The `qp` and
            # `qp_range` overrides that used to live here were removed --
            # both could disable that ceiling (see the note above).
            fps = body.get("fps")
            return self._json(200, start_stream(bitrate_kbps, max_seconds, fps))

        if self.path == "/stream/stop":
            return self._json(200, stop_stream("requested"))

        self._json(404, {"error": "not found"})


def run_server(picam2, api_key, port=PORT):
    """Blocks forever; call from a background thread. `picam2` must already
    be configured+started with the dual main+lores video configuration."""
    global _picam2
    _picam2 = picam2
    stream_storage.init_db(DB_PATH)

    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    server.api_key = api_key
    server.daemon_threads = True
    logger.info(f"Stream control server listening on :{port}")
    server.serve_forever()
