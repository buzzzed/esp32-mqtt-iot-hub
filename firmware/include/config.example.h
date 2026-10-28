// Copy this file to config.h (same folder) and fill in your values.
// config.h is listed in .gitignore so your credentials never get committed.
#pragma once

// --- Wi-Fi ---
#define WIFI_SSID     "your-wifi-ssid"
#define WIFI_PASSWORD "your-wifi-password"

// --- MQTT broker (the C++ broker on the Raspberry Pi) ---
#define MQTT_BROKER_IP   "192.168.1.50"
#define MQTT_BROKER_PORT 1883

// --- Node identity ---
// Must be unique per board: it is used in topic names and as the MQTT client ID
// (a broker kicks off an existing session when a second client reuses its ID).
// PlatformIO sets this per environment in platformio.ini; this is the fallback.
#ifndef NODE_ID
#define NODE_ID "node1"
#endif
