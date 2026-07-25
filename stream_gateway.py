#!/usr/bin/env python3
"""
On-demand live streaming for rancho-cam-pi.

Runs inside the SAME process (and same open Picamera2 instance) as
sender.py's periodic still-capture loop -- Picamera2/libcamera only allows
one process to hold the camera at a time (the "Device or resource busy"
lesson, same one already learned with the Growatt inverter's Modbus port),
so this is wired in as a background thread of sender.py's daemon rather
than a separate service. See stream_feasibility findings: the still
capture continues completely unaffected on the "main" stream while this
module drives an encoder on the concurrent "lores" stream.

Transport is MJPEG (multipart/x-mixed-replace), broadcast to however many
clients are connected (in practice just main-pi5, which fans it out
byte-for-byte to browser viewers -- see the monitor-cam-webapp side).
This replaced an earlier H264-over-ffmpeg-remux-into-fragmented-MP4 design
that main-pi5 fed through MediaSource/appendBuffer client-side: that
combination is a well-documented source of silent, hard-to-diagnose
failures (CHUNK_DEMUXER_ERROR_APPEND_FAILED and related stalls with no
error ever surfacing) and, on reconnect, never even got the request off
the browser reliably. MJPEG via a plain <img> tag needs no client-side
media pipeline at all -- multipart/x-mixed-replace has been handled
natively by every browser for decades, and each frame is retrieved from
each viewer, avoiding this whole bug class rather than patching around it.
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

from picamera2.encoders import MJPEGEncoder
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

# The camera's video configuration allows a 100us minimum frame duration,
# i.e. well over 100 fps -- measured at ~113 fps on real hardware. Left
# alone, the encoder spreads its bitrate budget across all of those frames
# and every single one comes out a ~5 KB, heavily-artifacted 640x480 JPEG
# ("looks like a Mario Bros video game"), while still pushing ~5 Mbps --
# more than double the ranch's entire uplink. Nobody needs 113 fps of a
# mostly-static landscape: capping the frame rate while streaming gives
# each frame a far larger share of the same budget (good-looking JPEGs)
# AND brings actual bandwidth in line with what was requested.
STREAM_FPS = 6
STREAM_FRAME_DURATION_US = int(1_000_000 / STREAM_FPS)
# Restored on stop -- must match sender.py's init_camera() configuration.
# Only the MINIMUM is raised while streaming: the 15.1s maximum has to
# stay put either way, since the night-mode still capture depends on it
# to take its 15s manual exposure.
IDLE_MIN_FRAME_DURATION_US = 100
MAX_FRAME_DURATION_US = 15_100_000

_CLIENT_QUEUE_MAXSIZE = 300  # ~a few seconds of buffered MJPEG at these bitrates

# multipart/x-mixed-replace boundary token, shared between the framing
# written here and the Content-Type header both this module's own HTTP
# handler and main-pi5's relay give to browsers -- must match exactly.
MULTIPART_BOUNDARY = b"FRAME"
MULTIPART_CONTENT_TYPE = f"multipart/x-mixed-replace; boundary={MULTIPART_BOUNDARY.decode()}"


class _Broadcaster(io.BufferedIOBase):
    """Fans out encoder output to every currently-connected HTTP client.
    A slow/stalled client gets its oldest buffered bytes dropped rather
    than blocking (or slowing down) the encoder thread for everyone else.

    Subclasses io.BufferedIOBase because picamera2's FileOutput requires
    it (raises "Must pass io.BufferedIOBase" otherwise) -- confirmed live
    the hard way when a plain object here silently killed every stream
    start attempt.

    Each write() call from the encoder is exactly one complete JPEG frame
    (picamera2's encoder/FileOutput contract) -- wrapping it in its own
    multipart part here, once, means every consumer (main-pi5's single
    upstream connection today, or a hypothetical direct viewer later)
    receives an already-correctly-framed byte stream it can just forward
    verbatim. A viewer joining mid-stream simply lands inside this
    boundary-delimited sequence and renders from the next full frame --
    multipart parsers (browsers included) treat anything before the first
    boundary match as discardable preamble, so no separate "cache the
    first chunk for late joiners" logic (needed for fMP4's init segment)
    is required here.
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
        frame = bytes(buf)
        packet = (
            b"--" + MULTIPART_BOUNDARY + b"\r\n"
            b"Content-Type: image/jpeg\r\n"
            b"Content-Length: " + str(len(frame)).encode() + b"\r\n\r\n" +
            frame + b"\r\n"
        )
        with self._lock:
            clients = list(self._clients)
            self.bytes_written += len(packet)
        for q in clients:
            try:
                q.put_nowait(packet)
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait(packet)
                except queue.Empty:
                    pass
        return len(frame)

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


def start_stream(bitrate_kbps, max_seconds):
    """Idempotent: returns the current session info whether or not this
    call actually started a new one."""
    bitrate_kbps = max(MIN_BITRATE_KBPS, min(MAX_BITRATE_KBPS, int(bitrate_kbps)))
    max_seconds = max(10, min(MAX_SESSION_SECONDS, int(max_seconds)))

    with _state.lock:
        if _state.active:
            return {"status": "already_active", "bitrate_kbps": _state.bitrate_kbps}

        broadcaster = _Broadcaster()
        # Cap the frame rate BEFORE starting the encoder so its rate control
        # is sized against the frame rate actually being produced.
        _picam2.set_controls({
            "FrameDurationLimits": (STREAM_FRAME_DURATION_US, MAX_FRAME_DURATION_US)
        })
        encoder = MJPEGEncoder(bitrate=bitrate_kbps * 1000)
        _picam2.start_encoder(encoder, FileOutput(broadcaster), name="lores")

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

        logger.info(f"Stream started @ {bitrate_kbps}kbps, hard cap {max_seconds}s")
        return {"status": "started", "bitrate_kbps": bitrate_kbps}


def stop_stream(reason="requested"):
    with _state.lock:
        _stop_locked(reason)
        return {"status": "stopped"}


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
        if self.path == "/health":
            return self._json(200, {"status": "ok", **status()})

        if self.path == "/stream/mjpeg":
            if not self._authed():
                return self._json(401, {"error": "bad api key"})
            with _state.lock:
                if not _state.active:
                    return self._json(409, {"error": "not streaming"})
                broadcaster = _state.broadcaster
                q = broadcaster.register()
            self.send_response(200)
            self.send_header("Content-Type", MULTIPART_CONTENT_TYPE)
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
            return self._json(200, start_stream(bitrate_kbps, max_seconds))

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
