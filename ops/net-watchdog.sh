#!/bin/bash
# Network watchdog for rancho-cam-pi
# - Pings gateway every 5 min (via systemd timer)
# - After 3 consecutive failures: restart NetworkManager (clears stuck wpa state)
# - After 6 consecutive failures: reboot the Pi
# - Counter persists in /var/lib/net-watchdog/fail-count

STATE_DIR=/var/lib/net-watchdog
STATE_FILE="$STATE_DIR/fail-count"
LOG_TAG=net-watchdog

mkdir -p "$STATE_DIR"
touch "$STATE_FILE"

GATEWAY=$(ip route | awk '/^default/ {print $3; exit}')
if [ -z "$GATEWAY" ]; then
  logger -t "$LOG_TAG" "no default route; treating as failure"
  GATEWAY=192.168.50.1
fi

if ping -c 2 -W 3 "$GATEWAY" >/dev/null 2>&1; then
  COUNT=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
  if [ "$COUNT" != "0" ]; then
    logger -t "$LOG_TAG" "gateway $GATEWAY back; clearing fail counter (was $COUNT)"
  fi
  echo 0 > "$STATE_FILE"
  exit 0
fi

COUNT=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
COUNT=$((COUNT + 1))
echo "$COUNT" > "$STATE_FILE"
logger -t "$LOG_TAG" "gateway $GATEWAY unreachable; fail count = $COUNT"

if [ "$COUNT" -ge 6 ]; then
  logger -t "$LOG_TAG" "fail count >= 6; rebooting"
  echo 0 > "$STATE_FILE"
  /sbin/reboot
elif [ "$COUNT" -ge 3 ]; then
  logger -t "$LOG_TAG" "fail count >= 3; restarting NetworkManager"
  systemctl restart NetworkManager
fi
