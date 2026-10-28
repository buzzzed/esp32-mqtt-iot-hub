"""Runtime settings, read from environment variables with sensible defaults."""

import os

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")  # broker runs on the same Pi
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "iot-hub-api")

DB_PATH = os.getenv("DB_PATH", "hub.db")

# A device whose last message is older than this is reported as offline, even
# if its retained status still says "online". Nodes heartbeat every 30 s, so
# 90 s tolerates two missed heartbeats.
OFFLINE_AFTER_SECONDS = int(os.getenv("OFFLINE_AFTER_SECONDS", "90"))

# Readings older than this many days are deleted (checked hourly) so the
# database on the Pi's SD card doesn't grow forever. 0 keeps everything.
READING_RETENTION_DAYS = float(os.getenv("READING_RETENTION_DAYS", "30"))
PRUNE_INTERVAL_SECONDS = int(os.getenv("PRUNE_INTERVAL_SECONDS", "3600"))
