#!/usr/bin/env python3
"""BME680 latch-up watchdog — runs every 5 min on rancho-cam-pi (timer).

Reads the BME680 chip-id (0x77 reg 0xD0) directly over I2C (no sudo; user is
in the i2c group). If the sensor is silent for FAIL_THRESHOLD consecutive runs
(~15 min) the bus is latched. It then, once (rate-limited):

  1. asks the steren-control agent on rancho-main-pi to power-cycle cam-pi's
     Steren plug (the agent waits for us to power off, cuts 5 min, powers on),
  2. shuts cam-pi down cleanly (sudo shutdown, NOPASSWD).

The agent handles the actual power cut (cam-pi can't cut its own power). Both
sides rate-limit so a dead sensor can't cause an endless power-cycle loop.

Token + agent URL live in ~/enviro-cam/.watchdog_env (KEY=VALUE).
"""
import json
import subprocess
import time
import urllib.request
from pathlib import Path

import smbus2

CFG = {}
_cfgp = Path.home() / "enviro-cam" / ".watchdog_env"
if _cfgp.exists():
    for line in _cfgp.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            CFG[k.strip()] = v.strip()

# No default: the agent's address is fleet topology and this repo is public.
# It is always set in .watchdog_env (see .watchdog_env.example); an empty
# value here means "not configured", which is handled in main().
AGENT_URL = CFG.get("AGENT_URL", "")
AGENT_TOKEN = CFG.get("AGENT_TOKEN", "")
FAIL_THRESHOLD = int(CFG.get("FAIL_THRESHOLD", "3"))
RECOVER_MIN_INTERVAL_H = float(CFG.get("RECOVER_MIN_INTERVAL_H", "12"))
STATE = Path.home() / ".bme680_latch_watchdog.json"
I2C_ADDR, CHIP_ID_REG, BME680_CHIP_ID = 0x77, 0xD0, 0x61


def sensor_alive():
    """Try a few times (avoid a false negative from bus contention with sender.py)."""
    for _ in range(3):
        try:
            bus = smbus2.SMBus(1)
            v = bus.read_byte_data(I2C_ADDR, CHIP_ID_REG)
            bus.close()
            if v == BME680_CHIP_ID:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def load():
    try:
        return json.loads(STATE.read_text())
    except Exception:
        return {}


def save(d):
    STATE.write_text(json.dumps(d))


def main():
    st = load()

    if sensor_alive():
        if st.get("fail_count", 0):
            st["fail_count"] = 0
            save(st)
        return

    st["fail_count"] = st.get("fail_count", 0) + 1
    save(st)
    if st["fail_count"] < FAIL_THRESHOLD:
        return  # not yet confirmed; wait for more consecutive misses

    # latch-up confirmed (N consecutive silent runs)
    last = st.get("last_recovery_ts", 0)
    if last and (time.time() - last) / 3600 < RECOVER_MIN_INTERVAL_H:
        return  # rate-limited locally; agent also guards + emails

    reason = f"cam-pi watchdog: BME680 mudo {st['fail_count']} ciclos (bus I2C latched)"
    if AGENT_URL:
        try:
            req = urllib.request.Request(
                AGENT_URL,
                data=json.dumps({"reason": reason}).encode(),
                headers={"X-Auth": AGENT_TOKEN, "Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass  # even if the agent call fails, shut down; a human will see the health alert

    st["last_recovery_ts"] = time.time()
    st["fail_count"] = 0
    save(st)
    time.sleep(6)  # let the agent register the request before we drop off the net
    subprocess.run(["sudo", "shutdown", "-h", "now"])


if __name__ == "__main__":
    main()
