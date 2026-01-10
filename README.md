# Enviro-Cam

A FastAPI web application serving BME680 sensor data and live camera stream on Raspberry Pi.

## Hardware Requirements

- Raspberry Pi 4 (or 5)
- Raspberry Pi Camera Module 3
- BME680 Sensor (I2C)
- Appropriate cables

## Wiring

### BME680 to Raspberry Pi

| BME680 Pin | Raspberry Pi Pin | GPIO |
|------------|------------------|------|
| VIN / VCC | Pin 1 or 17 | 3.3V |
| GND | Pin 6, 9, or 14 | GND |
| SCL | Pin 5 | GPIO 3 (SCL) |
| SDA | Pin 3 | GPIO 2 (SDA) |

### Camera Module 3

Connect via the CSI ribbon cable to the camera port.

## Setup Guide

### 1. Enable I2C

```bash
sudo raspi-config
# Navigate to: Interface Options -> I2C -> Enable
sudo reboot
```

### 2. Install System Dependencies

```bash
sudo apt install i2c-tools libcap-dev python3-dev python3-picamera2
```

### 3. Verify Hardware

Check sensor:
```bash
i2cdetect -y 1
# Should show 0x76 or 0x77
```

Check camera:
```bash
rpicam-hello --list-cameras
# Should show imx708 or similar
```

### 4. Create Project Structure

```bash
mkdir -p ~/enviro-cam
cd ~/enviro-cam
python -m venv venv --system-site-packages
source venv/bin/activate
```

### 5. Install Python Dependencies

```bash
pip install fastapi uvicorn bme680
```

### 6. Create the Application

Create `app.py` with the application code (see app.py in this directory).

### 7. Run the Application

```bash
cd ~/enviro-cam
source venv/bin/activate
python app.py
```

Expected output:
```
==================================================
Enviro-Cam
==================================================

Camera: 1280x720 OK
Sensor: BME680 at 0x77 OK

--------------------------------------------------
Available at:
  http://192.168.x.x:8080
  http://hostname.local:8080
--------------------------------------------------

API Endpoints:
  GET /              - Web dashboard
  GET /api/sensor    - Sensor data (JSON)
  GET /api/status    - System status (JSON)
  GET /stream.mjpg   - MJPEG video stream
--------------------------------------------------

Press Ctrl+C to stop.
```

## Running as a Service

Create the service file:
```bash
sudo nano /etc/systemd/system/enviro-cam.service
```

Paste:
```ini
[Unit]
Description=Enviro-Cam
After=network.target

[Service]
User=mafx
WorkingDirectory=/home/mafx/enviro-cam
ExecStart=/home/mafx/enviro-cam/venv/bin/python app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable enviro-cam
sudo systemctl start enviro-cam
```

Check status:
```bash
sudo systemctl status enviro-cam
```

## API Reference

### GET /api/sensor

Returns current sensor readings.

```json
{
  "temperature_c": 24.35,
  "temperature_f": 75.83,
  "humidity": 48.23,
  "pressure": 1013.25,
  "gas_resistance": 50000,
  "air_quality": 72.5,
  "heat_stable": true,
  "timestamp": "2026-01-08T12:34:56.789",
  "uptime_seconds": 3600,
  "reading_count": 1200
}
```

### GET /api/status

Returns system status.

```json
{
  "camera_available": true,
  "sensor_available": true,
  "camera_resolution": [1280, 720],
  "sensor_address": "0x77",
  "uptime_seconds": 3600,
  "hostname": "raspberrypi",
  "ip_address": "192.168.1.100"
}
```

### GET /stream.mjpg

Returns MJPEG video stream. Embed in HTML:

```html
<img src="http://hostname.local:8080/stream.mjpg" />
```

## Project Structure

```
~/enviro-cam/
├── venv/
├── app.py
└── README.md
```

## Configuration

Edit these values in `app.py` to customize:

```python
HOST = "0.0.0.0"
PORT = 8080
CAMERA_RESOLUTION = (1280, 720)
SENSOR_I2C_ADDRESS = 0x77  # Use 0x76 if SDO connected to GND
SENSOR_READ_INTERVAL = 3   # Seconds between readings
```

## Troubleshooting

### Sensor not detected
- Check wiring connections
- Verify I2C is enabled: `sudo raspi-config`
- Try alternate address (0x76 vs 0x77)
- Run `i2cdetect -y 1` to scan for devices

### Camera not working
- Check ribbon cable connection (blue side facing ethernet port)
- Verify camera is detected: `rpicam-hello --list-cameras`
- Ensure no other process is using the camera

### Service won't start
- Check logs: `journalctl -u enviro-cam -f`
- Verify paths in service file match your installation
- Ensure venv has all dependencies installed
