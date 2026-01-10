#!/usr/bin/env python
"""
Enviro-Cam - Environmental Sensor + Camera Stream
FastAPI web application for Raspberry Pi
"""

import io
import sys
import socket
import time
import sqlite3
import os
from threading import Condition, Thread, Lock
from contextlib import asynccontextmanager
from datetime import datetime

# FastAPI
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Camera
try:
    from picamera2 import Picamera2
    from picamera2.encoders import MJPEGEncoder
    from picamera2.outputs import FileOutput
    CAMERA_AVAILABLE = True
except ImportError:
    CAMERA_AVAILABLE = False
    print("Warning: picamera2 not available")

# Sensor
try:
    import bme680
    SENSOR_AVAILABLE = True
except ImportError:
    SENSOR_AVAILABLE = False
    print("Warning: bme680 not available")


# =============================================================================
# Configuration
# =============================================================================

HOST = "0.0.0.0"
PORT = 8080
CAMERA_RESOLUTION = (1280, 720)
SENSOR_I2C_ADDRESS = 0x77  # Use 0x76 if SDO connected to GND
SENSOR_READ_INTERVAL = 3   # Seconds between readings
HISTORY_INTERVAL = 300     # Seconds between history recordings (5 minutes)
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DATA_DIR, "sensor_history.db")


# =============================================================================
# Global State
# =============================================================================

picam2 = None
camera_output = None
sensor = None
sensor_data = {
    "temperature_c": None,
    "temperature_f": None,
    "humidity": None,
    "pressure": None,
    "gas_resistance": None,
    "air_quality": None,
    "heat_stable": False,
    "timestamp": None,
    "uptime_seconds": 0,
    "reading_count": 0,
    "error": None
}
sensor_lock = Lock()
start_time = None
last_history_save = 0


# =============================================================================
# Database Setup
# =============================================================================

def init_database():
    """Initialize SQLite database for sensor history."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            temperature_c REAL,
            temperature_f REAL,
            humidity REAL,
            pressure REAL,
            gas_resistance REAL,
            air_quality REAL
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON sensor_history(timestamp)')
    conn.commit()
    conn.close()
    print(f"Database: {DB_PATH} OK")


def save_history_point(data):
    """Save a data point to the history database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sensor_history
            (timestamp, temperature_c, temperature_f, humidity, pressure, gas_resistance, air_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get("timestamp"),
            data.get("temperature_c"),
            data.get("temperature_f"),
            data.get("humidity"),
            data.get("pressure"),
            data.get("gas_resistance"),
            data.get("air_quality")
        ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Database save error: {e}")
        return False


def get_history(hours=24):
    """Get historical data for the specified number of hours."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT timestamp, temperature_c, humidity, pressure, air_quality
            FROM sensor_history
            WHERE datetime(timestamp) >= datetime('now', 'localtime', ?)
            ORDER BY timestamp ASC
        ''', (f'-{hours} hours',))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Database read error: {e}")
        return []


def get_history_stats():
    """Get database statistics."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM sensor_history')
        total_points = cursor.fetchone()[0]
        cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM sensor_history')
        row = cursor.fetchone()
        conn.close()
        return {
            "total_points": total_points,
            "oldest": row[0],
            "newest": row[1]
        }
    except Exception as e:
        print(f"Database stats error: {e}")
        return {"total_points": 0, "oldest": None, "newest": None}


# =============================================================================
# Camera Setup
# =============================================================================

class StreamingOutput(io.BufferedIOBase):
    """Thread-safe streaming output buffer."""

    def __init__(self):
        self.frame = None
        self.condition = Condition()

    def write(self, buf):
        with self.condition:
            self.frame = buf
            self.condition.notify_all()


def setup_camera():
    """Initialize camera module."""
    global picam2, camera_output

    if not CAMERA_AVAILABLE:
        print("Camera: picamera2 not installed")
        return False

    try:
        picam2 = Picamera2()
        config = picam2.create_video_configuration(
            main={"size": CAMERA_RESOLUTION, "format": "RGB888"},
            buffer_count=4
        )
        picam2.configure(config)

        camera_output = StreamingOutput()
        encoder = MJPEGEncoder(10000000)
        picam2.start_recording(encoder, FileOutput(camera_output))

        print(f"Camera: {CAMERA_RESOLUTION[0]}x{CAMERA_RESOLUTION[1]} OK")
        return True
    except Exception as e:
        print(f"Camera: {e}")
        return False


def stop_camera():
    """Stop camera recording."""
    global picam2
    if picam2:
        picam2.stop_recording()


# =============================================================================
# Sensor Setup
# =============================================================================

def calculate_air_quality(gas_resistance, humidity):
    """
    Calculate simple air quality score (0-100).
    Higher = better air quality.
    """
    gas_baseline = 50000
    humidity_baseline = 40

    gas_score = min(gas_resistance / gas_baseline, 1.0) * 75

    humidity_offset = abs(humidity - humidity_baseline)
    if humidity_offset <= 10:
        humidity_score = 25
    elif humidity_offset <= 20:
        humidity_score = 25 - (humidity_offset - 10) * 1.5
    else:
        humidity_score = max(0, 25 - (humidity_offset - 10) * 1.5)

    return round(gas_score + humidity_score, 1)


def setup_sensor():
    """Initialize BME680 sensor."""
    global sensor

    if not SENSOR_AVAILABLE:
        print("Sensor: bme680 not installed")
        return False

    try:
        sensor = bme680.BME680(i2c_addr=SENSOR_I2C_ADDRESS)

        sensor.set_humidity_oversample(bme680.OS_2X)
        sensor.set_pressure_oversample(bme680.OS_4X)
        sensor.set_temperature_oversample(bme680.OS_8X)
        sensor.set_filter(bme680.FILTER_SIZE_3)

        sensor.set_gas_status(bme680.ENABLE_GAS_MEAS)
        sensor.set_gas_heater_temperature(320)
        sensor.set_gas_heater_duration(150)
        sensor.select_gas_heater_profile(0)

        print(f"Sensor: BME680 at 0x{SENSOR_I2C_ADDRESS:02x} OK")
        return True
    except Exception as e:
        print(f"Sensor: {e}")
        return False


def sensor_read_loop():
    """Background thread for continuous sensor readings."""
    global sensor, sensor_data, start_time, last_history_save

    reading_count = 0
    error_count = 0
    max_errors_before_reinit = 5

    while True:
        try:
            if sensor and sensor.get_sensor_data():
                reading_count += 1
                error_count = 0  # Reset on successful read
                current_time = time.time()

                with sensor_lock:
                    sensor_data["temperature_c"] = round(sensor.data.temperature, 2)
                    sensor_data["temperature_f"] = round(sensor.data.temperature * 9/5 + 32, 2)
                    sensor_data["humidity"] = round(sensor.data.humidity, 2)
                    sensor_data["pressure"] = round(sensor.data.pressure, 2)
                    sensor_data["heat_stable"] = sensor.data.heat_stable
                    sensor_data["timestamp"] = datetime.now().isoformat()
                    sensor_data["uptime_seconds"] = int(current_time - start_time)
                    sensor_data["reading_count"] = reading_count
                    sensor_data["error"] = None

                    if sensor.data.heat_stable:
                        sensor_data["gas_resistance"] = round(sensor.data.gas_resistance, 0)
                        sensor_data["air_quality"] = calculate_air_quality(
                            sensor.data.gas_resistance,
                            sensor.data.humidity
                        )
                    else:
                        sensor_data["gas_resistance"] = None
                        sensor_data["air_quality"] = None

                    # Save to history database every HISTORY_INTERVAL seconds
                    if current_time - last_history_save >= HISTORY_INTERVAL:
                        if sensor_data["temperature_c"] is not None:
                            save_history_point(sensor_data)
                            last_history_save = current_time

        except Exception as e:
            error_count += 1
            error_msg = f"Sensor error ({error_count}): {e}"
            print(error_msg)

            with sensor_lock:
                sensor_data["error"] = str(e)
                sensor_data["timestamp"] = datetime.now().isoformat()
                sensor_data["uptime_seconds"] = int(time.time() - start_time)

            # Try to reinitialize sensor after repeated failures
            if error_count >= max_errors_before_reinit:
                print("Attempting sensor reinitialization...")
                try:
                    sensor = bme680.BME680(i2c_addr=SENSOR_I2C_ADDRESS)
                    sensor.set_humidity_oversample(bme680.OS_2X)
                    sensor.set_pressure_oversample(bme680.OS_4X)
                    sensor.set_temperature_oversample(bme680.OS_8X)
                    sensor.set_filter(bme680.FILTER_SIZE_3)
                    sensor.set_gas_status(bme680.ENABLE_GAS_MEAS)
                    sensor.set_gas_heater_temperature(320)
                    sensor.set_gas_heater_duration(150)
                    sensor.select_gas_heater_profile(0)
                    print("Sensor reinitialized successfully")
                    error_count = 0
                except Exception as reinit_error:
                    print(f"Sensor reinitialization failed: {reinit_error}")
                    time.sleep(10)  # Wait longer before next attempt

        time.sleep(SENSOR_READ_INTERVAL)


# =============================================================================
# FastAPI Application
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown."""
    global start_time

    print("=" * 50)
    print("Enviro-Cam")
    print("=" * 50 + "\n")

    start_time = time.time()

    # Initialize database
    init_database()

    # Initialize hardware
    camera_ok = setup_camera()
    sensor_ok = setup_sensor()

    if not camera_ok and not sensor_ok:
        print("\nNo hardware available. Exiting.")
        sys.exit(1)

    # Start sensor background thread
    if sensor_ok:
        sensor_thread = Thread(target=sensor_read_loop, daemon=True)
        sensor_thread.start()

    # Display URLs
    ip = get_ip_address()
    hostname = socket.gethostname()

    print("\n" + "-" * 50)
    print("Available at:")
    print(f"  http://{ip}:{PORT}")
    print(f"  http://{hostname}.local:{PORT}")
    print("-" * 50)
    print("\nAPI Endpoints:")
    print(f"  GET /              - Web dashboard")
    print(f"  GET /api/sensor    - Sensor data (JSON)")
    print(f"  GET /api/status    - System status (JSON)")
    print(f"  GET /api/history   - Historical data (JSON)")
    print(f"  GET /stream.mjpg   - MJPEG video stream")
    print("-" * 50)
    print("\nPress Ctrl+C to stop.\n")

    yield

    # Cleanup
    print("\nShutting down...")
    stop_camera()
    print("Goodbye!")


def get_ip_address():
    """Get local IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


app = FastAPI(
    title="Enviro-Cam",
    description="Environmental sensor + camera stream",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for external API access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/api/sensor", response_class=JSONResponse)
async def api_sensor():
    """Return current sensor readings as JSON."""
    with sensor_lock:
        return sensor_data.copy()


@app.get("/api/status", response_class=JSONResponse)
async def api_status():
    """Return system status."""
    stats = get_history_stats()
    return {
        "camera_available": CAMERA_AVAILABLE and picam2 is not None,
        "sensor_available": SENSOR_AVAILABLE and sensor is not None,
        "camera_resolution": CAMERA_RESOLUTION if CAMERA_AVAILABLE else None,
        "sensor_address": f"0x{SENSOR_I2C_ADDRESS:02x}" if SENSOR_AVAILABLE else None,
        "uptime_seconds": int(time.time() - start_time) if start_time else 0,
        "hostname": socket.gethostname(),
        "ip_address": get_ip_address(),
        "history_points": stats["total_points"],
        "history_oldest": stats["oldest"],
        "history_newest": stats["newest"]
    }


@app.get("/api/history", response_class=JSONResponse)
async def api_history(hours: int = 24):
    """Return historical sensor data."""
    data = get_history(hours)
    return {
        "hours": hours,
        "points": len(data),
        "data": data
    }


@app.get("/stream.mjpg")
async def video_stream():
    """MJPEG video stream."""
    if not camera_output:
        return JSONResponse(
            status_code=503,
            content={"error": "Camera not available"}
        )

    def generate():
        while True:
            try:
                with camera_output.condition:
                    camera_output.condition.wait(timeout=5.0)
                    frame = camera_output.frame
                if frame:
                    yield (b"--FRAME\r\n"
                           b"Content-Type: image/jpeg\r\n\r\n" + frame + b"\r\n")
            except Exception as e:
                print(f"Stream error: {e}")
                time.sleep(1)  # Brief pause before retry

    return StreamingResponse(
        generate(),
        media_type="multipart/x-mixed-replace; boundary=FRAME"
    )


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Web dashboard."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enviro-Cam</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
    <style>
        :root {
            --bg-primary: #f9fafb;
            --bg-secondary: #ffffff;
            --bg-tertiary: #f3f4f6;
            --text-primary: #1f2937;
            --text-secondary: #6b7280;
            --text-muted: #9ca3af;
            --border-color: #e5e7eb;
            --accent: #2563eb;
            --accent-light: #3b82f6;
            --success: #10b981;
            --warning: #f59e0b;
            --error: #ef4444;
            --shadow: 0 1px 3px rgba(0,0,0,0.1), 0 1px 2px rgba(0,0,0,0.06);
            --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.1), 0 4px 6px -2px rgba(0,0,0,0.05);
        }

        [data-theme="dark"] {
            --bg-primary: #111827;
            --bg-secondary: #1f2937;
            --bg-tertiary: #374151;
            --text-primary: #f9fafb;
            --text-secondary: #d1d5db;
            --text-muted: #9ca3af;
            --border-color: #374151;
            --accent: #3b82f6;
            --accent-light: #60a5fa;
            --shadow: 0 1px 3px rgba(0,0,0,0.3), 0 1px 2px rgba(0,0,0,0.2);
            --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.3), 0 4px 6px -2px rgba(0,0,0,0.2);
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Ubuntu", sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            transition: background 0.3s ease, color 0.3s ease;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 24px;
        }

        /* Header */
        header {
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-light) 100%);
            border-radius: 16px;
            padding: 24px 32px;
            margin-bottom: 24px;
            box-shadow: var(--shadow-lg);
            animation: slideIn 0.4s ease;
        }

        @keyframes slideIn {
            from { opacity: 0; transform: translateY(-10px); }
            to { opacity: 1; transform: translateY(0); }
        }

        .header-content {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 16px;
        }

        .header-title {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        header h1 {
            color: #fff;
            font-size: 28px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            background: rgba(255,255,255,0.2);
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 500;
            color: #fff;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #4ade80;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .header-info {
            color: rgba(255,255,255,0.9);
            font-size: 14px;
        }

        .header-controls {
            display: flex;
            gap: 12px;
            align-items: center;
        }

        /* Theme Toggle */
        .theme-toggle {
            background: rgba(255,255,255,0.2);
            border: none;
            border-radius: 8px;
            padding: 8px 12px;
            color: #fff;
            cursor: pointer;
            font-size: 18px;
            transition: background 0.2s ease;
        }

        .theme-toggle:hover {
            background: rgba(255,255,255,0.3);
        }

        /* Grid Layout */
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 24px;
            animation: fadeIn 0.5s ease;
        }

        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        /* Cards */
        .card {
            background: var(--bg-secondary);
            border-radius: 12px;
            padding: 24px;
            border: 1px solid var(--border-color);
            box-shadow: var(--shadow);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }

        .card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-lg);
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border-color);
        }

        .card h2 {
            font-size: 18px;
            font-weight: 600;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .card-icon {
            font-size: 20px;
        }

        /* Camera Feed */
        .camera-container {
            position: relative;
            border-radius: 8px;
            overflow: hidden;
            background: #000;
        }

        .camera-feed {
            width: 100%;
            display: block;
            border-radius: 8px;
        }

        .camera-overlay {
            position: absolute;
            top: 12px;
            left: 12px;
            background: rgba(0,0,0,0.6);
            backdrop-filter: blur(4px);
            padding: 6px 10px;
            border-radius: 6px;
            font-size: 12px;
            font-weight: 500;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .live-dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: #ef4444;
            animation: pulse 1s infinite;
        }

        /* Sensor Grid */
        .sensor-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }

        .sensor-item {
            background: var(--bg-tertiary);
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            transition: transform 0.2s ease;
        }

        .sensor-item:hover {
            transform: scale(1.02);
        }

        .sensor-icon {
            font-size: 24px;
            margin-bottom: 8px;
        }

        .sensor-value {
            font-size: 32px;
            font-weight: 700;
            font-family: "SF Mono", "Fira Code", monospace;
            color: var(--accent);
            line-height: 1.2;
        }

        .sensor-unit {
            font-size: 14px;
            font-weight: 500;
            color: var(--text-secondary);
            margin-left: 2px;
        }

        .sensor-label {
            font-size: 13px;
            font-weight: 500;
            color: var(--text-muted);
            margin-top: 6px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        /* Air Quality Bar */
        .aqi-container {
            margin-top: 8px;
        }

        .aqi-bar {
            height: 6px;
            background: var(--border-color);
            border-radius: 3px;
            overflow: hidden;
        }

        .aqi-fill {
            height: 100%;
            border-radius: 3px;
            transition: width 0.5s ease, background 0.3s ease;
        }

        .aqi-excellent { background: linear-gradient(90deg, #10b981, #34d399); }
        .aqi-good { background: linear-gradient(90deg, #84cc16, #a3e635); }
        .aqi-fair { background: linear-gradient(90deg, #f59e0b, #fbbf24); }
        .aqi-poor { background: linear-gradient(90deg, #ef4444, #f87171); }

        /* Warning Banner */
        .warning-banner {
            background: linear-gradient(135deg, rgba(245,158,11,0.15) 0%, rgba(245,158,11,0.1) 100%);
            border: 1px solid rgba(245,158,11,0.3);
            color: var(--warning);
            padding: 12px 16px;
            border-radius: 8px;
            text-align: center;
            font-size: 14px;
            font-weight: 500;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }

        /* Status Bar */
        .status-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 20px;
            padding-top: 16px;
            border-top: 1px solid var(--border-color);
            font-size: 13px;
            color: var(--text-muted);
        }

        .status-item {
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .status-value {
            font-weight: 600;
            font-family: "SF Mono", "Fira Code", monospace;
            color: var(--text-secondary);
        }

        /* Footer */
        footer {
            margin-top: 32px;
            padding: 20px;
            text-align: center;
            border-top: 1px solid var(--border-color);
        }

        .api-links {
            display: flex;
            justify-content: center;
            gap: 24px;
            flex-wrap: wrap;
        }

        .api-link {
            color: var(--text-muted);
            text-decoration: none;
            font-size: 13px;
            font-family: "SF Mono", "Fira Code", monospace;
            padding: 6px 12px;
            border-radius: 6px;
            background: var(--bg-tertiary);
            transition: all 0.2s ease;
        }

        .api-link:hover {
            color: var(--accent);
            background: var(--bg-secondary);
        }

        /* Chart Card */
        .chart-card {
            grid-column: 1 / -1;
        }

        .chart-container {
            position: relative;
            height: 300px;
            width: 100%;
        }

        .chart-controls {
            display: flex;
            gap: 8px;
            margin-bottom: 16px;
            flex-wrap: wrap;
        }

        .chart-btn {
            padding: 8px 16px;
            border: 1px solid var(--border-color);
            border-radius: 6px;
            background: var(--bg-tertiary);
            color: var(--text-secondary);
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .chart-btn:hover {
            border-color: var(--accent);
            color: var(--accent);
        }

        .chart-btn.active {
            background: var(--accent);
            border-color: var(--accent);
            color: #fff;
        }

        .chart-stats {
            display: flex;
            gap: 24px;
            margin-top: 16px;
            padding-top: 16px;
            border-top: 1px solid var(--border-color);
            font-size: 13px;
            color: var(--text-muted);
            flex-wrap: wrap;
        }

        .chart-stat {
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .chart-stat-value {
            font-weight: 600;
            font-family: "SF Mono", "Fira Code", monospace;
            color: var(--text-secondary);
        }

        /* Responsive */
        @media (max-width: 768px) {
            .container { padding: 16px; }
            header { padding: 20px; border-radius: 12px; }
            header h1 { font-size: 22px; }
            .header-content { flex-direction: column; align-items: flex-start; }
            .sensor-value { font-size: 26px; }
            .grid { grid-template-columns: 1fr; }
            .chart-container { height: 250px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-content">
                <div class="header-title">
                    <h1>Enviro-Cam</h1>
                    <div class="status-badge">
                        <span class="status-dot"></span>
                        <span>Live</span>
                    </div>
                </div>
                <div class="header-info" id="hostname">Loading...</div>
                <div class="header-controls">
                    <button class="theme-toggle" onclick="toggleTheme()" title="Toggle theme">
                        <span id="theme-icon">&#9790;</span>
                    </button>
                </div>
            </div>
        </header>

        <div class="grid">
            <div class="card">
                <div class="card-header">
                    <h2><span class="card-icon">&#128249;</span> Camera</h2>
                </div>
                <div class="camera-container">
                    <img class="camera-feed" src="/stream.mjpg" alt="Camera Stream" />
                    <div class="camera-overlay">
                        <span class="live-dot"></span>
                        <span>1280x720</span>
                    </div>
                </div>
            </div>

            <div class="card">
                <div class="card-header">
                    <h2><span class="card-icon">&#127777;</span> Environment</h2>
                </div>
                <div id="heat-warning" class="warning-banner" style="display: none;">
                    &#9203; Gas sensor warming up (~5 min)
                </div>
                <div class="sensor-grid">
                    <div class="sensor-item">
                        <div class="sensor-icon">&#127777;</div>
                        <div class="sensor-value"><span id="temp-c">--</span><span class="sensor-unit">C</span></div>
                        <div class="sensor-label">Temperature</div>
                    </div>
                    <div class="sensor-item">
                        <div class="sensor-icon">&#128167;</div>
                        <div class="sensor-value"><span id="humidity">--</span><span class="sensor-unit">%</span></div>
                        <div class="sensor-label">Humidity</div>
                    </div>
                    <div class="sensor-item">
                        <div class="sensor-icon">&#127744;</div>
                        <div class="sensor-value"><span id="pressure">--</span><span class="sensor-unit">hPa</span></div>
                        <div class="sensor-label">Pressure</div>
                    </div>
                    <div class="sensor-item">
                        <div class="sensor-icon">&#127811;</div>
                        <div class="sensor-value"><span id="aqi">--</span><span class="sensor-unit">/100</span></div>
                        <div class="sensor-label">Air Quality</div>
                        <div class="aqi-container">
                            <div class="aqi-bar">
                                <div class="aqi-fill" id="aqi-bar" style="width: 0%"></div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="status-bar">
                    <div class="status-item">
                        <span>Reading</span>
                        <span class="status-value">#<span id="reading-count">0</span></span>
                    </div>
                    <div class="status-item">
                        <span>Uptime</span>
                        <span class="status-value" id="uptime">0s</span>
                    </div>
                </div>
            </div>

            <div class="card chart-card">
                <div class="card-header">
                    <h2><span class="card-icon">&#128200;</span> History</h2>
                </div>
                <div class="chart-controls">
                    <button class="chart-btn" onclick="loadHistory(1)">1H</button>
                    <button class="chart-btn" onclick="loadHistory(6)">6H</button>
                    <button class="chart-btn active" onclick="loadHistory(24)">24H</button>
                    <button class="chart-btn" onclick="loadHistory(72)">3D</button>
                    <button class="chart-btn" onclick="loadHistory(168)">7D</button>
                    <button class="chart-btn" onclick="loadHistory(720)">30D</button>
                </div>
                <div class="chart-container">
                    <canvas id="historyChart"></canvas>
                </div>
                <div class="chart-stats">
                    <div class="chart-stat">
                        <span>Data points:</span>
                        <span class="chart-stat-value" id="chart-points">0</span>
                    </div>
                    <div class="chart-stat">
                        <span>Total stored:</span>
                        <span class="chart-stat-value" id="chart-total">0</span>
                    </div>
                </div>
            </div>
        </div>

        <footer>
            <div class="api-links">
                <a href="/api/sensor" class="api-link">/api/sensor</a>
                <a href="/api/status" class="api-link">/api/status</a>
                <a href="/api/history" class="api-link">/api/history</a>
                <a href="/stream.mjpg" class="api-link">/stream.mjpg</a>
            </div>
        </footer>
    </div>

    <script>
        // Theme handling
        function getPreferredTheme() {
            const saved = localStorage.getItem('theme');
            if (saved) return saved;
            return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
        }

        function setTheme(theme) {
            document.documentElement.setAttribute('data-theme', theme);
            localStorage.setItem('theme', theme);
            document.getElementById('theme-icon').innerHTML = theme === 'dark' ? '&#9788;' : '&#9790;';
        }

        function toggleTheme() {
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            setTheme(current === 'dark' ? 'light' : 'dark');
        }

        setTheme(getPreferredTheme());

        // Utility functions
        function formatUptime(seconds) {
            const h = Math.floor(seconds / 3600);
            const m = Math.floor((seconds % 3600) / 60);
            const s = seconds % 60;
            if (h > 0) return `${h}h ${m}m`;
            if (m > 0) return `${m}m ${s}s`;
            return `${s}s`;
        }

        function getAqiClass(aqi) {
            if (aqi >= 80) return 'aqi-excellent';
            if (aqi >= 60) return 'aqi-good';
            if (aqi >= 40) return 'aqi-fair';
            return 'aqi-poor';
        }

        async function updateSensor() {
            try {
                const res = await fetch('/api/sensor');
                const data = await res.json();

                document.getElementById('temp-c').textContent =
                    data.temperature_c !== null ? data.temperature_c.toFixed(1) : '--';
                document.getElementById('humidity').textContent =
                    data.humidity !== null ? data.humidity.toFixed(1) : '--';
                document.getElementById('pressure').textContent =
                    data.pressure !== null ? data.pressure.toFixed(0) : '--';
                document.getElementById('reading-count').textContent = data.reading_count;
                document.getElementById('uptime').textContent = formatUptime(data.uptime_seconds);

                const heatWarning = document.getElementById('heat-warning');
                const aqiValue = document.getElementById('aqi');
                const aqiBar = document.getElementById('aqi-bar');

                if (data.heat_stable && data.air_quality !== null) {
                    heatWarning.style.display = 'none';
                    aqiValue.textContent = data.air_quality.toFixed(0);
                    aqiBar.style.width = data.air_quality + '%';
                    aqiBar.className = 'aqi-fill ' + getAqiClass(data.air_quality);
                } else {
                    heatWarning.style.display = 'flex';
                    aqiValue.textContent = '--';
                    aqiBar.style.width = '0%';
                }
            } catch (e) {
                console.error('Sensor update failed:', e);
            }
        }

        async function updateStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                document.getElementById('hostname').textContent =
                    `${data.hostname} | ${data.ip_address}`;
                document.getElementById('chart-total').textContent =
                    data.history_points ? data.history_points.toLocaleString() : '0';
            } catch (e) {
                console.error('Status update failed:', e);
            }
        }

        // Chart setup
        let historyChart = null;
        let currentHours = 24;

        function getChartColors() {
            const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
            return {
                temp: { line: '#ef4444', bg: 'rgba(239, 68, 68, 0.1)' },
                humidity: { line: '#3b82f6', bg: 'rgba(59, 130, 246, 0.1)' },
                grid: isDark ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)',
                text: isDark ? '#9ca3af' : '#6b7280'
            };
        }

        async function loadHistory(hours) {
            currentHours = hours;

            // Update active button
            document.querySelectorAll('.chart-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');

            try {
                const res = await fetch(`/api/history?hours=${hours}`);
                const result = await res.json();

                document.getElementById('chart-points').textContent = result.points.toLocaleString();

                if (result.data.length === 0) {
                    return;
                }

                const labels = result.data.map(d => new Date(d.timestamp));
                const temps = result.data.map(d => d.temperature_c);
                const humidity = result.data.map(d => d.humidity);

                const colors = getChartColors();
                const ctx = document.getElementById('historyChart').getContext('2d');

                if (historyChart) {
                    historyChart.destroy();
                }

                historyChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Temperature (C)',
                                data: temps,
                                borderColor: colors.temp.line,
                                backgroundColor: colors.temp.bg,
                                borderWidth: 2,
                                fill: true,
                                tension: 0.3,
                                pointRadius: hours <= 6 ? 3 : 0,
                                pointHoverRadius: 5,
                                yAxisID: 'y'
                            },
                            {
                                label: 'Humidity (%)',
                                data: humidity,
                                borderColor: colors.humidity.line,
                                backgroundColor: colors.humidity.bg,
                                borderWidth: 2,
                                fill: true,
                                tension: 0.3,
                                pointRadius: hours <= 6 ? 3 : 0,
                                pointHoverRadius: 5,
                                yAxisID: 'y1'
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: {
                            mode: 'index',
                            intersect: false
                        },
                        plugins: {
                            legend: {
                                position: 'top',
                                labels: { color: colors.text, usePointStyle: true }
                            },
                            tooltip: {
                                backgroundColor: 'rgba(0,0,0,0.8)',
                                titleColor: '#fff',
                                bodyColor: '#fff',
                                callbacks: {
                                    title: (items) => {
                                        const date = new Date(items[0].parsed.x);
                                        return date.toLocaleString();
                                    }
                                }
                            }
                        },
                        scales: {
                            x: {
                                type: 'time',
                                time: {
                                    unit: hours <= 6 ? 'minute' : hours <= 24 ? 'hour' : 'day',
                                    displayFormats: {
                                        minute: 'HH:mm',
                                        hour: 'HH:mm',
                                        day: 'MMM d'
                                    }
                                },
                                grid: { color: colors.grid },
                                ticks: { color: colors.text, maxTicksLimit: 8 }
                            },
                            y: {
                                type: 'linear',
                                position: 'left',
                                title: { display: true, text: 'Temp (C)', color: colors.text },
                                grid: { color: colors.grid },
                                ticks: { color: colors.temp.line }
                            },
                            y1: {
                                type: 'linear',
                                position: 'right',
                                title: { display: true, text: 'Humidity (%)', color: colors.text },
                                grid: { drawOnChartArea: false },
                                ticks: { color: colors.humidity.line },
                                min: 0,
                                max: 100
                            }
                        }
                    }
                });
            } catch (e) {
                console.error('History load failed:', e);
            }
        }

        // Refresh chart when theme changes
        const originalToggleTheme = toggleTheme;
        toggleTheme = function() {
            originalToggleTheme();
            if (historyChart) {
                loadHistory(currentHours);
            }
        };

        updateStatus();
        updateSensor();
        loadHistory(24);
        setInterval(updateSensor, 3000);
        setInterval(() => loadHistory(currentHours), 300000); // Refresh chart every 5 min
    </script>
</body>
</html>
"""


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
