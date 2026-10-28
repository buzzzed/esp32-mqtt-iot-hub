"""The hub's MQTT client: listens to the broker and records what devices say.

Topic conventions (shared with the ESP32 firmware):

    devices/<id>/status          "online" / "offline"  (retained)
    sensors/<id>/<kind>          JSON readings, e.g. sensors/node1/env
    actuators/<id>/<name>        commands TO the device, e.g. "on"
    actuators/<id>/<name>/state  state reported BY the device (retained)

paho-mqtt runs its network loop in a background thread (loop_start()), and all
callbacks below execute on that thread. It also reconnects automatically, and
because we subscribe inside on_connect, subscriptions are restored after every
reconnect.
"""

import logging
import time
from typing import Optional

import paho.mqtt.client as mqtt

from .hub_storage import Database
from .rules_engine import RulesEngine

log = logging.getLogger(__name__)


class MqttBridge:
    def __init__(self, db: Database, host: str, port: int, client_id: str):
        self._db = db
        self._host = host
        self._port = port

        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311,  # what the broker speaks
            clean_session=True,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        # Back off 1 s, 2 s, 4 s ... up to 30 s between reconnect attempts.
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)

        self.rules = RulesEngine(db, publish=self.publish)

    # --------------------------------------------------------------- lifecycle

    def start(self) -> None:
        # connect_async + loop_start: don't block (or crash) API startup if the
        # broker isn't up yet; the background loop keeps retrying.
        self._client.connect_async(self._host, self._port, keepalive=30)
        self._client.loop_start()

    def stop(self) -> None:
        self._client.disconnect()
        self._client.loop_stop()

    @property
    def connected(self) -> bool:
        return self._client.is_connected()

    def publish(self, topic: str, payload: str, qos: int = 1,
                retain: bool = False) -> bool:
        """Queue a message for the broker. Returns False if we're offline."""
        if not self.connected:
            return False
        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        return info.rc == mqtt.MQTT_ERR_SUCCESS

    # --------------------------------------------------------- paho callbacks

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT connect refused: %s", reason_code)
            return
        log.info("connected to MQTT broker %s:%d", self._host, self._port)
        # "#" = every topic. We need sensor/status/state topics for storage,
        # and rules may watch any topic a user chooses, so take everything.
        client.subscribe("#", qos=1)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        log.warning("disconnected from MQTT broker (%s); will retry", reason_code)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        # Never let one bad message raise out of the callback and disturb the
        # network thread; log it and move on.
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
            self.handle_message(msg.topic, payload, retained=bool(msg.retain))
        except Exception:
            log.exception("error handling message on %s", msg.topic)

    # --------------------------------------------------------- message logic
    # Kept separate from the paho callback so it can be unit-tested directly.

    def handle_message(self, topic: str, payload: str, retained: bool,
                       now: Optional[float] = None) -> None:
        """Route one message: update device status, store readings, run rules.

        Retained messages are what the broker replays when we (re)subscribe.
        They describe the past, so we record the state they carry but do NOT
        treat them as proof a device is alive, and do NOT run rules on them
        (otherwise every hub restart would re-trigger actions).
        """
        now = time.time() if now is None else now
        parts = topic.split("/")

        if len(parts) == 3 and parts[0] == "devices" and parts[2] == "status":
            status = payload.strip()[:32] or "unknown"
            self._db.set_device_status(parts[1], status,
                                       seen_at=None if retained else now)

        elif _is_reading_topic(parts) and not retained:
            device_id = parts[1]
            # Any data from a device proves it is alive right now.
            self._db.set_device_status(device_id, "online", seen_at=now)
            self._db.insert_reading(device_id, topic, payload, now)

        if not retained:
            self.rules.evaluate(topic, payload, now)


def _is_reading_topic(parts: list[str]) -> bool:
    """sensors/<id>/<kind>  or  actuators/<id>/<name>/state"""
    if len(parts) == 3 and parts[0] == "sensors":
        return True
    return len(parts) == 4 and parts[0] == "actuators" and parts[3] == "state"
