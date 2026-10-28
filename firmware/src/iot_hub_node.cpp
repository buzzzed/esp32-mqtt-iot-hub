/*
 * IoT hub node firmware: ESP32 + Arduino framework + PubSubClient.
 *
 * One source file, two node types (pick one with a #define):
 *
 *   NODE_SENSOR   reads a DHT22 every 5 s and publishes
 *                   sensors/<node_id>/env   {"temp":22.4,"humidity":41.0}
 *
 *   NODE_ACTUATOR subscribes to
 *                   actuators/<node_id>/led     payload "on" / "off"
 *                 and reports its LED state (retained) on
 *                   actuators/<node_id>/led/state
 *
 * Both node types report presence on devices/<node_id>/status:
 *   - "online"  (retained) right after every successful MQTT connect,
 *               and again every 30 s as a heartbeat;
 *   - "offline" (retained) published *by the broker* as our Last Will if we
 *               vanish without a clean disconnect (power loss, Wi-Fi drop, crash).
 * The heartbeat is the backup: even if the will is never delivered, the hub
 * marks a device offline once its heartbeats stop arriving.
 *
 * Design rule: loop() never blocks. Wi-Fi and MQTT reconnects are attempted on
 * timers, so a missing network never freezes the rest of the program.
 */

#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>

#include "config.h"  // Wi-Fi credentials, broker IP, NODE_ID (gitignored)

// ---------------------------------------------------------------------------
// Node type selection. PlatformIO passes -D NODE_SENSOR / -D NODE_ACTUATOR;
// in the Arduino IDE, change the default below.
// ---------------------------------------------------------------------------
#if !defined(NODE_SENSOR) && !defined(NODE_ACTUATOR)
#define NODE_SENSOR  // <-- change to NODE_ACTUATOR for an LED node
#endif

#if defined(NODE_SENSOR) && defined(NODE_ACTUATOR)
#error "Define exactly one of NODE_SENSOR or NODE_ACTUATOR"
#endif

#ifdef NODE_SENSOR
#include <DHT.h>
#endif

// ---------------------------------------------------------------------------
// Pins and timing
// ---------------------------------------------------------------------------
static const uint8_t DHT_PIN = 4;  // DHT22 data pin (with a 10k pull-up to 3.3V)
static const uint8_t LED_PIN = 2;  // on-board LED on most ESP32 DevKit boards

static const unsigned long WIFI_RETRY_MS      = 10000;  // between Wi-Fi attempts
static const unsigned long MQTT_RETRY_MS      = 5000;   // between MQTT attempts
static const unsigned long HEARTBEAT_MS       = 30000;  // "online" heartbeat period
static const unsigned long SENSOR_INTERVAL_MS = 5000;   // DHT22 read period
static const uint16_t      MQTT_KEEPALIVE_S   = 15;     // sent in CONNECT

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------
static WiFiClient   wifiClient;        // the TCP socket...
static PubSubClient mqtt(wifiClient);  // ...that the MQTT client talks over

// Topic strings are built once in setup() from NODE_ID.
static char clientId[32];
static char statusTopic[64];
#ifdef NODE_SENSOR
static char envTopic[64];
static DHT  dht(DHT_PIN, DHT22);
#endif
#ifdef NODE_ACTUATOR
static char ledTopic[64];
static char ledStateTopic[64];
static bool ledOn = false;
#endif

// Timestamps from millis(). Intervals are checked as (now - last >= period),
// which stays correct when millis() wraps around after ~49 days because the
// subtraction is done in unsigned arithmetic.
static unsigned long lastWifiAttempt = 0;
static unsigned long lastMqttAttempt = 0;
static unsigned long lastHeartbeat   = 0;
#ifdef NODE_SENSOR
static unsigned long lastSensorRead = 0;
#endif

// ---------------------------------------------------------------------------
// Actuator helpers
// ---------------------------------------------------------------------------
#ifdef NODE_ACTUATOR
// Report the LED state, retained, so anyone who subscribes later (e.g. the
// hub after a restart) immediately learns the current state.
static void publishLedState() {
  mqtt.publish(ledStateTopic, ledOn ? "on" : "off", /*retained=*/true);
}

static void setLed(bool on) {
  ledOn = on;
  digitalWrite(LED_PIN, on ? HIGH : LOW);
  Serial.printf("[led] %s\n", on ? "ON" : "OFF");
  publishLedState();
}
#endif

// ---------------------------------------------------------------------------
// MQTT callbacks
// ---------------------------------------------------------------------------

// Called from inside mqtt.loop() whenever a subscribed message arrives.
//
// Gotcha: PubSubClient uses ONE internal buffer for both incoming and outgoing
// packets, and `topic`/`payload` point into it. Calling mqtt.publish() here
// overwrites that buffer, so we copy/compare everything we need first and only
// publish (inside setLed) at the very end.
static void onMqttMessage(char* topic, byte* payload, unsigned int length) {
#ifdef NODE_ACTUATOR
  if (strcmp(topic, ledTopic) != 0) return;

  // The payload is NOT null-terminated: copy it into a bounded C string.
  char msg[16];
  unsigned int n = length < sizeof(msg) - 1 ? length : sizeof(msg) - 1;
  memcpy(msg, payload, n);
  msg[n] = '\0';

  // Normalise: lowercase, and strip trailing whitespace/newlines
  // (command-line tools often append a '\n').
  for (unsigned int i = 0; i < n; i++) msg[i] = tolower((unsigned char)msg[i]);
  while (n > 0 && isspace((unsigned char)msg[n - 1])) msg[--n] = '\0';

  if (strcmp(msg, "on") == 0) {
    setLed(true);
  } else if (strcmp(msg, "off") == 0) {
    setLed(false);
  } else {
    Serial.printf("[mqtt] ignoring unknown LED command '%s'\n", msg);
  }
#else
  // Sensor nodes don't subscribe to anything.
  (void)topic; (void)payload; (void)length;
#endif
}

// Runs once after every successful (re)connect. With a clean session the broker
// forgets our subscriptions when we disconnect, so we must resubscribe here.
static void onMqttConnected() {
  Serial.println("[mqtt] connected");
  mqtt.publish(statusTopic, "online", /*retained=*/true);
  lastHeartbeat = millis();

#ifdef NODE_ACTUATOR
  mqtt.subscribe(ledTopic, /*qos=*/1);  // QoS 1: commands are delivered at least once
  publishLedState();
#endif
}

// ---------------------------------------------------------------------------
// Connection management (non-blocking)
// ---------------------------------------------------------------------------

// Returns true when Wi-Fi is connected. Otherwise starts a fresh connection
// attempt at most once every WIFI_RETRY_MS and returns false immediately.
static bool ensureWifi() {
  if (WiFi.status() == WL_CONNECTED) return true;

  unsigned long now = millis();
  if (now - lastWifiAttempt >= WIFI_RETRY_MS) {
    lastWifiAttempt = now;
    Serial.println("[wifi] not connected, retrying...");
    WiFi.disconnect();
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  }
  return false;
}

// Returns true when the MQTT session is up. Otherwise tries to connect at most
// once every MQTT_RETRY_MS. (connect() itself blocks briefly while the TCP
// handshake and CONNECT/CONNACK exchange happen, but only for one attempt.)
static bool ensureMqtt() {
  if (mqtt.connected()) return true;

  unsigned long now = millis();
  if (now - lastMqttAttempt < MQTT_RETRY_MS) return false;
  lastMqttAttempt = now;

  Serial.printf("[mqtt] connecting to %s:%d as %s...\n",
                MQTT_BROKER_IP, MQTT_BROKER_PORT, clientId);

  // The last three arguments register our Last Will and Testament: if the
  // broker loses us without a DISCONNECT packet (detected via the keep-alive
  // timeout), it publishes "offline" to statusTopic, retained, at QoS 1.
  bool ok = mqtt.connect(clientId,
                         /*willTopic=*/statusTopic,
                         /*willQos=*/1,
                         /*willRetain=*/true,
                         /*willMessage=*/"offline");
  if (!ok) {
    // state() codes: -4 timeout, -2 connect failed, 1..5 CONNACK refusals
    Serial.printf("[mqtt] connect failed, state=%d\n", mqtt.state());
    return false;
  }
  onMqttConnected();
  return true;
}

// ---------------------------------------------------------------------------
// Periodic work
// ---------------------------------------------------------------------------
static void sendHeartbeat() {
  mqtt.publish(statusTopic, "online", /*retained=*/true);
#ifdef NODE_ACTUATOR
  publishLedState();  // lets the hub refresh "latest state" without a command
#endif
}

#ifdef NODE_SENSOR
static void publishSensorReading() {
  float temp = dht.readTemperature();  // degrees C
  float hum  = dht.readHumidity();     // % RH
  if (isnan(temp) || isnan(hum)) {
    // DHT22s occasionally return a bad read (timing glitch); just skip it.
    Serial.println("[dht] read failed, skipping");
    return;
  }

  char json[64];
  snprintf(json, sizeof(json), "{\"temp\":%.1f,\"humidity\":%.1f}", temp, hum);
  // Not retained: these are a stream of measurements, not state.
  mqtt.publish(envTopic, json);
  Serial.printf("[dht] %s -> %s\n", envTopic, json);
}
#endif

// ---------------------------------------------------------------------------
// Arduino entry points
// ---------------------------------------------------------------------------
void setup() {
  Serial.begin(115200);
  delay(100);
  Serial.printf("\nBooting node '%s'\n", NODE_ID);

  // Build client ID and topics from NODE_ID.
  snprintf(clientId,    sizeof(clientId),    "esp32-%s", NODE_ID);
  snprintf(statusTopic, sizeof(statusTopic), "devices/%s/status", NODE_ID);
#ifdef NODE_SENSOR
  snprintf(envTopic, sizeof(envTopic), "sensors/%s/env", NODE_ID);
  dht.begin();
#endif
#ifdef NODE_ACTUATOR
  snprintf(ledTopic,      sizeof(ledTopic),      "actuators/%s/led", NODE_ID);
  snprintf(ledStateTopic, sizeof(ledStateTopic), "actuators/%s/led/state", NODE_ID);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
#endif

  // Wi-Fi: station mode. We own the reconnect logic in ensureWifi(), so the
  // core's built-in auto-reconnect is turned off to avoid the two fighting.
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(false);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  lastWifiAttempt = millis();

  mqtt.setServer(MQTT_BROKER_IP, MQTT_BROKER_PORT);
  mqtt.setCallback(onMqttMessage);
  mqtt.setKeepAlive(MQTT_KEEPALIVE_S);
}

void loop() {
  // Each layer only runs once the layer below it is up:
  // Wi-Fi -> MQTT -> application work.
  if (ensureWifi() && ensureMqtt()) {
    mqtt.loop();  // send PINGREQs, read incoming packets, fire onMqttMessage

    unsigned long now = millis();
    if (now - lastHeartbeat >= HEARTBEAT_MS) {
      lastHeartbeat = now;
      sendHeartbeat();
    }
#ifdef NODE_SENSOR
    if (now - lastSensorRead >= SENSOR_INTERVAL_MS) {
      lastSensorRead = now;
      publishSensorReading();
    }
#endif
  }

  delay(10);  // yield a little so we don't spin the CPU at 100%
}
