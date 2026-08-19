# Deployment Notes — rancho-cam-pi

Production deployment of enviro-cam on a Raspberry Pi 4 in a remote cabin (rancho), reachable only over Tailscale. This document captures the operational setup that lives **outside** the application code: the sender daemon, the network watchdog, and the recovery procedure when the Pi goes offline.

## Topology

- Hardware: Raspberry Pi 4B, 4 GB RAM, Debian 13 (trixie)
- Network: WiFi only, behind an ASUS RT-AC1200 V2 (MediaTek chipset) on `192.168.50.0/24`
- Remote access: Tailscale (DERP relay — high latency, no LAN access)
- Sensors: BME680 over I2C (`0x77`), Pi Camera Module 3 (CSI), one or more ESP32s discovered on LAN
- Power: solar/inverter — uptime is at the mercy of the cabin power loop

## Components running on the Pi

| Unit | Source | Purpose |
|------|--------|---------|
| `enviro-cam.service` | this repo (`app.py`) | FastAPI dashboard + MJPEG stream on :8080 |
| `sensor-sender.service` | this repo (`sender.py`) | Every 5 min: read BME680, snap camera, discover ESP32s, POST to remote receiver |
| `bme680-latch-watchdog.timer` | this repo (`bme680_latch_watchdog.py`) | Every 5 min: checks the BME680 answers on I2C; on a confirmed latch, asks for a power cycle and shuts down |
| `net-watchdog.timer` | system-level (see below) | Watchdog that recovers from stuck WiFi states |
| `tailscaled.service` | tailscale package | Remote access tunnel |

## sender.py

The 5-minute push pipeline. Reads `sender_config.json` for the receiver URL and API key — **this file is gitignored**; copy `sender_config.example.json` and fill it in before first run.

```bash
cp sender_config.example.json sender_config.json
# edit receiver_url, api_key, device_id
sudo cp sensor-sender.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sensor-sender
```

`esp32_known_ips.json` is also gitignored — it's runtime state, populated by the discovery loop.

## BME680 latch-up watchdog

The BME680 has latched its I2C bus twice — it stops answering entirely and
nothing short of removing power brings it back. A reboot does not do it: the
3.3 V rail stays up across a reboot, so the chip stays latched. Only cutting
power works.

The Pi cannot cut its own power, which is the whole shape of this thing: the
watchdog detects the latch here, asks a `steren-control` agent on another
ranch Pi to cycle cam-pi's Steren plug, and then shuts itself down cleanly so
the agent's cut lands on a Pi that is already off.

```bash
cp .watchdog_env.example .watchdog_env   # gitignored: holds the agent token
# fill in AGENT_URL and AGENT_TOKEN
sudo cp bme680-latch-watchdog.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bme680-latch-watchdog.timer
```

Detection reads the chip-id register (0x77 / 0xD0, expects 0x61) directly over
I2C — no sudo needed, the user is in the `i2c` group. Two guards keep a dead
sensor from cycling the power forever:

- **Three consecutive silent runs (~15 min)** before declaring a latch. One
  miss is usually bus contention with `sender.py`'s own read, not a latch, and
  each check already retries three times internally.
- **A 12-hour floor between recovery attempts**, held in
  `~/.bme680_latch_watchdog.json`. The agent rate-limits independently, so
  neither side alone can run away.

The agent call is best-effort: if it fails the Pi still shuts down, because
the health alert from main-pi5 will surface a Pi that stopped reporting far
more reliably than one that is up with a dead sensor.

Being `Type=oneshot` on a timer, **`inactive (dead)` is its healthy state** —
the same trap that made `net-watchdog` show a permanent red DOWN on the fleet
dashboard. Check `systemctl list-timers bme680-latch-watchdog.timer`, not
`is-active`.

## Network watchdog (system-level, not in this repo)

The Pi sits on a flaky WiFi link and we are not physically present. A simple watchdog runs every 5 minutes via systemd timer:

- Pings the default gateway
- 3 consecutive failures → `systemctl restart NetworkManager` (clears stuck wpa_supplicant state)
- 6 consecutive failures → `reboot`
- Counter persists across runs in `/var/lib/net-watchdog/fail-count`

Files:
- `/usr/local/sbin/net-watchdog.sh` (the script)
- `/etc/systemd/system/net-watchdog.service` (Type=oneshot)
- `/etc/systemd/system/net-watchdog.timer` (OnBootSec=3min, OnUnitActiveSec=5min)

Reference copies are checked into the `ops/` directory of this repo.

## The 2.4 GHz HT-cap incompatibility (real story behind the 33-day outage)

In April 2026 the cam-pi went offline for 33 days. Root cause turned out to be a firmware-level incompatibility, not a hardware failure:

- Pi 4 BCM4345 (`brcmfmac`) WiFi chip sends an 802.11n HT Capabilities IE that the ASUS RT-AC1200's MediaTek 2.4 GHz radio firmware rejects with `PeerAssocReqSanity - IE_HT_CAP`.
- Each rapid reboot leaves a stale STA entry on the AP; until it ages out (~1 h), reassociation keeps failing.
- The 5 GHz radio (Realtek, different code path) has no such problem.

Mitigations applied:
1. `apt full-upgrade` — picked up newer `firmware-brcm80211`.
2. Watchdog above — recovers automatically from future stuck states without a site visit.
3. Prefer 5 GHz association where signal allows.

## Recovery procedure if the Pi goes offline

1. Wait at least 1 hour after any prior reboot attempt (let the AP's stale STA entry age out).
2. If reachable on Tailscale: `ssh mafx@<tailnet-ip>` and check `journalctl -u net-watchdog` for the failure pattern.
3. If unreachable: a single power-cycle is usually enough once the AP-side state has aged out. The watchdog will keep it healthy after that.
4. If still failing, the BCM/MTK HT-cap mismatch may be back — workarounds: force-disable 802.11n on the 2.4 GHz radio in the router admin, or move the Pi closer to the 5 GHz radio.
