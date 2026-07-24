#!/usr/bin/env python3
"""
Sensor Data Sender for rancho-cam-pi

Pushes sensor readings and camera images to the central receiver.
Deploy to: /home/mafx/enviro-cam/sender.py on rancho-cam-pi

Features:
- In-memory queuing for offline resilience (no SD card wear)
- Clock-aligned collection (every 5 min at :00, :05, :10, etc.)
- Timestamps at collection time (not send time)
- Independent sensor and image pipelines

Usage:
    python3 sender.py           # Run once
    python3 sender.py --daemon  # Run continuously every 5 minutes
"""

import argparse
import io
import json
import logging
import socket
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests

import stream_gateway

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration
CONFIG_PATH = Path(__file__).parent / "sender_config.json"

# ESP32 Discovery Settings
ESP32_SCAN_TIMEOUT = 1.0  # HTTP timeout for ESP32 requests
ESP32_KNOWN_IPS_FILE = Path(__file__).parent / "esp32_known_ips.json"

# BME680 Configuration
SENSOR_I2C_ADDRESS = 0x77  # Use 0x76 if SDO connected to GND

# Queue Configuration
MAX_SENSOR_QUEUE = 500   # ~41 hours at 5-min intervals (~1 MB RAM)
MAX_IMAGE_QUEUE = 288    # ~24 hours at 5-min intervals (~60-150 MB RAM)

# Global sensor instance (reuse to keep heater stable)
_sensor = None

# Global camera instance (reuse to avoid repeated init)
_picam2 = None

# In-memory queues for offline resilience
sensor_queue = deque(maxlen=MAX_SENSOR_QUEUE)
image_queue = deque(maxlen=MAX_IMAGE_QUEUE)
queue_lock = Lock()


def load_config():
    """Load configuration from JSON file."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        return json.load(f)


def get_local_ip():
    """Get the local IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "192.168.1.1"


def init_sensor():
    """Initialize BME680 sensor."""
    global _sensor
    if _sensor is not None:
        return _sensor

    try:
        import bme680

        # Try primary address (0x77), then secondary (0x76)
        for addr in [0x77, 0x76]:
            try:
                _sensor = bme680.BME680(i2c_addr=addr)
                logger.info(f"BME680 found at address 0x{addr:02x}")
                break
            except (IOError, OSError) as e:
                logger.debug(f"BME680 not at 0x{addr:02x}: {e}")
                continue
        else:
            raise IOError("BME680 not found at 0x77 or 0x76")

        # Configure sensor
        _sensor.set_humidity_oversample(bme680.OS_2X)
        _sensor.set_pressure_oversample(bme680.OS_4X)
        _sensor.set_temperature_oversample(bme680.OS_8X)
        _sensor.set_filter(bme680.FILTER_SIZE_3)
        _sensor.set_gas_status(bme680.ENABLE_GAS_MEAS)
        _sensor.set_gas_heater_temperature(320)
        _sensor.set_gas_heater_duration(150)
        _sensor.select_gas_heater_profile(0)

        return _sensor

    except ImportError:
        logger.error("bme680 library not installed")
        return None
    except Exception as e:
        logger.error(f"Failed to initialize BME680: {e}")
        return None


def init_camera():
    """Initialize Pi camera (reused across captures).

    Uses a dual-stream video configuration (not create_still_configuration)
    so the on-demand stream_gateway can run a concurrent low-bitrate H264
    encoder on the "lores" stream while this still-capture path keeps using
    "main" exactly as before -- picamera2/libcamera only allow one process
    to hold the camera, so both jobs have to share a single open instance.

    FrameDurationLimits is widened to 15.1s (matching the low-light manual
    exposure below) because a video configuration otherwise caps exposure
    to a fast video frame rate by default, which would silently break the
    15s night-mode capture. Confirmed on real hardware: the still capture
    at "main" is unaffected in normal (fast/auto) daylight conditions.
    """
    global _picam2
    if _picam2 is not None:
        return _picam2

    try:
        from picamera2 import Picamera2

        _picam2 = Picamera2()
        # 1280x720 main for stills (unchanged file size, ~100-200KB); a
        # smaller 640x480 lores stream feeds the optional live stream.
        config = _picam2.create_video_configuration(
            main={"size": (1280, 720)},
            lores={"size": (640, 480)},
            encode="lores",
            controls={"FrameDurationLimits": (100, 15_100_000)},
        )
        _picam2.configure(config)
        _picam2.start()
        time.sleep(2)  # Allow camera to warm up
        logger.info("Camera initialized (dual-stream: main 1280x720 + lores 640x480)")
        return _picam2

    except ImportError:
        logger.error("picamera2 not available - install with: sudo apt install python3-picamera2")
        return None
    except Exception as e:
        logger.error(f"Error initializing camera: {e}")
        return None


def calculate_air_quality(gas_resistance):
    """
    Calculate air quality score (0-100) from raw gas resistance alone.
    Higher score = better air quality.

    Previously (community algorithm from G6EJD/BME680-Example) this also
    scored humidity, treating 38-42% RH as "optimal" and penalizing any
    deviation -- a reasonable proxy for INDOOR comfort, but wrong applied
    to this OUTDOOR sensor: a rainy or dewy morning would drag the score
    down as if it were pollution, when it's just weather. Dropped
    2026-07-24; see homelab-map/devices/rancho-cam-pi.md for the full
    rationale and the planned proper fix (humidity-compensated gas
    resistance via absolute humidity, once enough raw gas_resistance
    history has accumulated to calibrate it for this specific sensor --
    raw values are now persisted in sensor_readings.gas_resistance for
    exactly that purpose).

    Gas resistance typical ranges:
    - Clean air: 50,000 - 500,000+ ohms
    - Polluted air: 10,000 - 50,000 ohms
    """
    gas_lower_limit = 10000   # Poor air quality
    gas_upper_limit = 300000  # Excellent air quality

    if gas_resistance >= gas_upper_limit:
        score = 100.0
    elif gas_resistance <= gas_lower_limit:
        score = 0.0
    else:
        score = ((gas_resistance - gas_lower_limit) /
                 (gas_upper_limit - gas_lower_limit)) * 100.0

    return round(max(0.0, min(100.0, score)), 1)


def get_local_sensor_data():
    """Read data from local BME680 sensor."""
    sensor = init_sensor()
    if sensor is None:
        return None

    try:
        # Take readings until gas heater is stable
        # With heater kept warm during sleep, this should be fast (1-2 attempts)
        # Without warm heater, may take up to 10 attempts (~10 seconds)
        got_data = False
        heat_stable = False

        for attempt in range(10):
            if sensor.get_sensor_data():
                got_data = True
                if sensor.data.heat_stable:
                    heat_stable = True
                    logger.debug(f"Heater stable after {attempt + 1} attempt(s)")
                    break  # Got stable gas reading
            time.sleep(1)

        if not got_data:
            logger.error("Failed to get any sensor data")
            return None

        # Use the data from the last successful reading
        data = {
            "temperature_c": round(sensor.data.temperature, 2),
            "humidity": round(sensor.data.humidity, 2),
            "pressure": round(sensor.data.pressure, 2),
            "air_quality": None,
            "gas_resistance": None,
        }

        if heat_stable and sensor.data.gas_resistance:
            data["gas_resistance"] = round(sensor.data.gas_resistance, 1)
            data["air_quality"] = calculate_air_quality(sensor.data.gas_resistance)
            logger.info(f"Sensor: {data['temperature_c']}C, {data['humidity']}% RH, "
                       f"{data['pressure']} hPa, AQ: {data['air_quality']}%, "
                       f"gas: {data['gas_resistance']:.0f}ohm")
        else:
            logger.info(f"Sensor: {data['temperature_c']}C, {data['humidity']}% RH, "
                       f"{data['pressure']} hPa (gas not stable yet)")

        return data

    except Exception as e:
        logger.error(f"Error reading sensor: {e}")
        return None


def check_esp32_device(ip: str):
    """Check if an IP hosts an ESP32 sensor device."""
    try:
        # ESP32 devices expose /status endpoint
        url = f"http://{ip}/status"
        req = urllib.request.Request(url, headers={"User-Agent": "EnviroCam/1.0"})
        with urllib.request.urlopen(req, timeout=ESP32_SCAN_TIMEOUT) as response:
            data = json.loads(response.read().decode())
            # Verify it's an ESP32 monitor device (must have these fields)
            if "firmwareVersion" in data and "sensorTemperature" in data and "macAddress" in data:
                return {
                    "ip": ip,
                    "name": data.get("deviceName", "Unknown"),
                    "mac": data.get("macAddress", ""),
                    "temperature_c": data.get("sensorTemperature"),
                    "humidity": data.get("ahtHumidity"),
                    "pressure": data.get("bmpPressure"),
                }
    except Exception:
        pass
    return None


def scan_esp32_devices():
    """Scan local network for ESP32 sensor devices."""
    devices = []

    # Load known IPs if available
    known_ips = []
    if ESP32_KNOWN_IPS_FILE.exists():
        try:
            with open(ESP32_KNOWN_IPS_FILE) as f:
                known_ips = json.load(f)
        except Exception:
            pass

    # First check known IPs
    for ip in known_ips:
        device = check_esp32_device(ip)
        if device:
            devices.append(device)
            logger.info(f"ESP32: Found '{device['name']}' at {ip}")

    # If no known IPs or no devices found, scan the network
    if not devices:
        local_ip = get_local_ip()
        base_ip = ".".join(local_ip.split(".")[:-1])

        logger.info(f"ESP32: Scanning {base_ip}.1-254...")

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(check_esp32_device, f"{base_ip}.{i}"): i
                      for i in range(1, 255)}

            for future in as_completed(futures, timeout=10):
                try:
                    device = future.result()
                    if device:
                        devices.append(device)
                        logger.info(f"ESP32: Found '{device['name']}' at {device['ip']}")
                except Exception:
                    pass

        # Save discovered IPs for next time
        if devices:
            try:
                with open(ESP32_KNOWN_IPS_FILE, "w") as f:
                    json.dump([d["ip"] for d in devices], f)
            except Exception:
                pass

    return devices


def get_esp32_data():
    """Get data from ESP32 devices on the network."""
    try:
        devices = scan_esp32_devices()
        # Convert to the format expected by the receiver
        return [
            {
                "name": d.get("name", "Unknown"),
                "mac": d.get("mac", ""),
                "temperature_c": d.get("temperature_c"),
                "humidity": d.get("humidity"),
                "pressure": d.get("pressure"),
            }
            for d in devices
        ]
    except Exception as e:
        logger.error(f"Error scanning ESP32 devices: {e}")
        return []


def capture_image_to_memory():
    """
    Capture image with adaptive exposure for low light.
    When auto-exposure uses high gain (>4), switch to manual long exposure.
    Based on Pi Forums research: 15s exposure with gain 8 works well for night.

    Note: AeExposureMode.Long is broken (GitHub #1324) - auto-exposure prefers
    high gain over long exposure. Manual mode is the only reliable approach.
    """
    picam2 = init_camera()
    if picam2 is None:
        return None

    try:
        # Let auto-exposure analyze the scene first
        time.sleep(0.5)
        metadata = picam2.capture_metadata()
        auto_exposure = metadata.get("ExposureTime", 0)  # microseconds
        auto_gain = metadata.get("AnalogueGain", 1.0)

        # If gain > 4, auto-exposure is struggling with low light
        # Switch to manual mode with research-backed settings
        is_low_light = auto_gain > 4.0

        if is_low_light:
            # Research-recommended night settings:
            # - 15 seconds exposure (research suggests 10-15s for night landscapes)
            # - Gain 8 (balances brightness vs noise, max is 16)
            target_exposure = 15000000  # 15 seconds in microseconds
            target_gain = 8.0

            picam2.set_controls({
                "AeEnable": False,
                "ExposureTime": target_exposure,
                "AnalogueGain": target_gain
            })
            # In the continuous video-mode pipeline (needed for the
            # concurrent stream feed), a frame shot under the OLD fast/auto
            # settings can still be queued when we ask for one -- capture
            # would just hand that stale frame back instantly instead of
            # a genuine 15s exposure. Wait past the requested exposure time
            # so the frame we grab is guaranteed to be a fresh one shot
            # under the new manual settings. Confirmed on hardware: without
            # this wait, AnalogueGain silently reports ~1.1 (auto's last
            # value) instead of the requested 8.0.
            time.sleep(target_exposure / 1_000_000 + 1)
            logger.info(f"Low light detected (gain={auto_gain:.1f}) -> using 15s manual exposure")

        # Capture the image
        buffer = io.BytesIO()
        picam2.capture_file(buffer, format='jpeg')
        image_bytes = buffer.getvalue()

        # Get actual capture metadata
        final_metadata = picam2.capture_metadata()
        exposure_s = final_metadata.get("ExposureTime", 0) / 1000000
        gain = final_metadata.get("AnalogueGain", 1.0)

        # Restore auto-exposure for next cycle
        if is_low_light:
            picam2.set_controls({"AeEnable": True})

        size_kb = len(image_bytes) / 1024
        logger.info(f"Image captured ({size_kb:.1f} KB, {exposure_s:.2f}s, gain={gain:.1f})")

        return image_bytes

    except Exception as e:
        logger.error(f"Error capturing image: {e}")
        try:
            picam2.set_controls({"AeEnable": True})
        except Exception:
            pass
        return None


# =============================================================================
# Queue Management Functions
# =============================================================================

def queue_sensor_data(payload):
    """Add sensor data to the queue."""
    with queue_lock:
        sensor_queue.append(payload)
        logger.debug(f"Sensor queued, queue size: {len(sensor_queue)}")


def queue_image(entry):
    """Add image to the queue."""
    with queue_lock:
        image_queue.append(entry)
        logger.debug(f"Image queued, queue size: {len(image_queue)}")


def get_queue_status():
    """Get current queue sizes."""
    with queue_lock:
        return len(sensor_queue), len(image_queue)


# =============================================================================
# Push Functions (Single Item)
# =============================================================================

def push_single_sensor(config, payload):
    """Push a single sensor reading to the receiver. Returns True on success."""
    headers = {
        "X-API-Key": config["api_key"],
        "Content-Type": "application/json"
    }
    url = f"{config['receiver_url']}/api/push/sensors"

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        logger.info(f"Sensor data pushed (ts={payload['timestamp']}): {response.json()}")
        return True
    except requests.exceptions.RequestException as e:
        logger.warning(f"Sensor push failed: {e}")
        return False


def push_single_image(config, entry):
    """Push a single image to the receiver. Returns True on success."""
    headers = {
        "X-API-Key": config["api_key"]
    }
    url = f"{config['receiver_url']}/api/push/image"

    try:
        files = {"image": ("capture.jpg", entry["image_bytes"], "image/jpeg")}
        data = {
            "device_id": entry["device_id"],
            "timestamp": entry["timestamp"]
        }
        # Longer timeout for image upload on unstable network
        response = requests.post(url, files=files, data=data, headers=headers, timeout=120)
        response.raise_for_status()
        logger.info(f"Image pushed (ts={entry['timestamp']}): {response.json()}")
        return True
    except requests.exceptions.RequestException as e:
        logger.warning(f"Image push failed: {e}")
        return False


# =============================================================================
# Queue Flush Functions (FIFO, Independent Pipelines)
# =============================================================================

def flush_sensor_queue(config):
    """
    Send oldest sensor readings first, stop on first failure.
    Independent of image queue - sensor failures don't block images.
    """
    sent_count = 0
    with queue_lock:
        queue_size = len(sensor_queue)

    while True:
        # Get oldest item without removing it yet
        with queue_lock:
            if not sensor_queue:
                break
            payload = sensor_queue[0]

        # Try to send (outside lock)
        if push_single_sensor(config, payload):
            # Success - remove from queue
            with queue_lock:
                if sensor_queue and sensor_queue[0] == payload:
                    sensor_queue.popleft()
            sent_count += 1
        else:
            # Network down - stop trying, keep in queue for next cycle
            break

    if sent_count > 0 or queue_size > 0:
        with queue_lock:
            remaining = len(sensor_queue)
        logger.info(f"Sensor queue: sent {sent_count}, remaining {remaining}")


def flush_image_queue(config):
    """
    Send oldest images first, stop on first failure.
    Independent of sensor queue - image failures don't block sensors.
    """
    sent_count = 0
    with queue_lock:
        queue_size = len(image_queue)

    while True:
        # Get oldest item without removing it yet
        with queue_lock:
            if not image_queue:
                break
            entry = image_queue[0]

        # Try to send (outside lock)
        if push_single_image(config, entry):
            # Success - remove from queue
            with queue_lock:
                if image_queue and image_queue[0] == entry:
                    image_queue.popleft()
            sent_count += 1
        else:
            # Network down - stop trying, keep in queue for next cycle
            break

    if sent_count > 0 or queue_size > 0:
        with queue_lock:
            remaining = len(image_queue)
        logger.info(f"Image queue: sent {sent_count}, remaining {remaining}")


# =============================================================================
# Clock-Aligned Timing Functions
# =============================================================================

def get_next_aligned_time(interval_minutes):
    """
    Return the next clock-aligned time (e.g., :00, :05, :10 for 5-min intervals).
    """
    now = datetime.now()
    current_minute = now.minute
    current_second = now.second
    current_micro = now.microsecond

    # Find the next aligned minute
    next_minute = ((current_minute // interval_minutes) + 1) * interval_minutes

    # Handle hour rollover
    if next_minute >= 60:
        next_hour = now.hour + 1
        next_minute = next_minute - 60
        if next_hour >= 24:
            next_hour = 0
            next_time = now.replace(hour=next_hour, minute=next_minute, second=0, microsecond=0)
            # Add a day
            from datetime import timedelta
            next_time = next_time + timedelta(days=1)
        else:
            next_time = now.replace(hour=next_hour, minute=next_minute, second=0, microsecond=0)
    else:
        next_time = now.replace(minute=next_minute, second=0, microsecond=0)

    return next_time


def sleep_until(target_time, keep_heater_warm=True, heater_interval=30):
    """
    Sleep until the target datetime, optionally keeping BME680 heater warm.

    Args:
        target_time: datetime to sleep until
        keep_heater_warm: if True, poll sensor every heater_interval seconds
        heater_interval: seconds between heater maintenance polls (default 30)
    """
    now = datetime.now()
    delta = (target_time - now).total_seconds()

    if delta <= 0:
        return

    logger.info(f"Sleeping until {target_time.strftime('%H:%M:%S')} ({delta:.0f}s)")

    if not keep_heater_warm:
        time.sleep(delta)
        return

    # Sleep in intervals, polling sensor to keep gas heater warm
    sensor = init_sensor()
    while True:
        now = datetime.now()
        remaining = (target_time - now).total_seconds()

        if remaining <= 0:
            break

        # Sleep for heater_interval or remaining time, whichever is shorter
        sleep_time = min(heater_interval, remaining)
        time.sleep(sleep_time)

        # Poll sensor to keep heater warm (don't log, just maintain temperature)
        if sensor and remaining > heater_interval:
            try:
                sensor.get_sensor_data()
            except Exception:
                pass  # Ignore errors during maintenance polls


# =============================================================================
# Main Run Functions
# =============================================================================

def run_once(config: dict):
    """Run a single data collection and push cycle."""
    logger.info("=" * 50)
    logger.info("Starting data collection...")

    # Timestamp at collection time (not send time)
    timestamp = datetime.now(timezone.utc).isoformat()
    device_id = config.get("device_id", "rancho-cam-pi")

    # Collect sensor data
    sensor_data = get_local_sensor_data()
    esp32_data = get_esp32_data()

    # Queue sensor data
    if sensor_data or esp32_data:
        sensor_payload = {
            "device_id": device_id,
            "timestamp": timestamp,
            "local_sensor": sensor_data or {},
            "esp32_devices": esp32_data or []
        }
        queue_sensor_data(sensor_payload)
    else:
        logger.warning("No sensor data to queue")

    # Capture and queue image (directly to memory, no SD write)
    image_bytes = capture_image_to_memory()
    if image_bytes:
        image_entry = {
            "device_id": device_id,
            "timestamp": timestamp,
            "image_bytes": image_bytes
        }
        queue_image(image_entry)

    # Try to flush queues (independent pipelines)
    flush_sensor_queue(config)
    flush_image_queue(config)

    # Log queue status
    sensor_count, image_count = get_queue_status()
    if sensor_count > 0 or image_count > 0:
        logger.info(f"Pending: {sensor_count} sensor readings, {image_count} images")

    logger.info("Data collection complete")


def run_daemon(config: dict, interval_minutes: int = 5):
    """
    Run continuously at clock-aligned intervals.
    Collection happens at :00, :05, :10 (not "5 min from start").
    """
    logger.info(f"Starting daemon mode, interval: {interval_minutes} minutes")
    logger.info(f"Receiver URL: {config['receiver_url']}")
    logger.info(f"Queue limits: {MAX_SENSOR_QUEUE} sensors, {MAX_IMAGE_QUEUE} images")

    # Initialize camera once at startup
    picam2 = init_camera()

    # Start the on-demand stream control server as a background thread of
    # this same process -- it shares the single open Picamera2 instance
    # above rather than opening its own (only one process/owner allowed).
    stream_api_key = config.get("stream_api_key")
    if picam2 is not None and stream_api_key:
        t = threading.Thread(
            target=stream_gateway.run_server,
            args=(picam2, stream_api_key),
            daemon=True,
        )
        t.start()
    elif picam2 is not None:
        logger.warning("stream_api_key not set in sender_config.json -- live streaming disabled")

    while True:
        try:
            # Sleep until next aligned time
            next_time = get_next_aligned_time(interval_minutes)
            sleep_until(next_time)

            # Run collection cycle
            run_once(config)

        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in run cycle: {e}")
            # On error, sleep a bit before trying again
            time.sleep(30)


def main():
    parser = argparse.ArgumentParser(description="Push sensor data to receiver")
    parser.add_argument("--daemon", action="store_true",
                       help="Run continuously every 5 minutes")
    parser.add_argument("--interval", type=int, default=5,
                       help="Interval in minutes for daemon mode (default: 5)")
    args = parser.parse_args()

    try:
        config = load_config()
    except FileNotFoundError as e:
        logger.error(str(e))
        logger.info("Create sender_config.json with receiver_url and api_key")
        return 1

    if args.daemon:
        run_daemon(config, args.interval)
    else:
        run_once(config)

    return 0


if __name__ == "__main__":
    exit(main())
