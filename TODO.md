# TODO

## ESP32 Device Identification Improvements

### Problem
Currently using IP address as the primary identifier for ESP32 devices. IPs can change with DHCP, making device tracking unreliable over time.

### Solution
Use MAC address as the unique device identifier since it's hardware-bound and never changes.

### Changes Required

#### 1. esp32-monitor repo (marcofariasmx/esp32-monitor)
- Modify `/status` endpoint to include `macAddress` field in the JSON response
- Example response:
```json
{
  "deviceName": "Living Room",
  "macAddress": "AA:BB:CC:DD:EE:FF",
  "mdnsHostname": "esp32-livingroom.local",
  "firmwareVersion": "1.2.3",
  "sensorTemperature": 22.5,
  "sensorHumidity": 55.0,
  "sensorPressure": 1015.0
}
```

#### 2. enviro-cam repo (this repo)
- Update `esp32_devices` table schema:
  - Add `mac_address TEXT UNIQUE NOT NULL` column
  - Change primary key from `ip` to `mac_address`
  - Keep `ip` as a non-unique field (can change)
- Update `esp32_history` table:
  - Change `device_ip` to `device_mac`
  - Update foreign key reference
- Update `probe_esp32_device()` to extract MAC from response
- Update `scan_network_for_esp32()` to use MAC as key
- Update API endpoints to support filtering by MAC
- Update dashboard to display MAC address
