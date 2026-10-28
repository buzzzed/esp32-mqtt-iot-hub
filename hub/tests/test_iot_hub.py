"""Tests for the rules engine, message routing and REST API.

No broker needed: messages are injected straight into MqttBridge.handle_message,
and publishing is replaced with a fake that records what would have been sent.

Run from the hub/ directory:  pytest -q
"""

import pytest
from fastapi.testclient import TestClient

from iot_hub import hub_api as main, hub_settings as settings
from iot_hub.hub_storage import Database
from iot_hub.mqtt_bridge import MqttBridge
from iot_hub.rules_engine import RulesEngine, extract_field

HOT_RULE = {
    "name": "fan on when hot", "topic": "sensors/node1/env", "field": "temp",
    "operator": ">", "value": 28, "action_topic": "actuators/node2/led",
    "action_payload": "on", "cooldown_seconds": 30, "enabled": True,
}


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "test.db"))
    yield database
    database.close()


class FakePublisher:
    def __init__(self, ok=True):
        self.ok = ok
        self.sent = []

    def __call__(self, topic, payload, qos=1, retain=False):
        self.sent.append((topic, payload))
        return self.ok


# ------------------------------------------------------------ rules engine

def test_extract_field():
    assert extract_field('{"temp": 22.4}', "temp") == 22.4
    assert extract_field('{"env": {"temp": 5}}', "env.temp") == 5.0
    assert extract_field('{"temp": "hot"}', "temp") is None
    assert extract_field('{"temp": true}', "temp") is None
    assert extract_field("not json", "temp") is None
    assert extract_field('{"humidity": 40}', "temp") is None


def test_rule_fires_and_respects_cooldown(db):
    pub = FakePublisher()
    engine = RulesEngine(db, pub)
    rule_id = db.create_rule(HOT_RULE)["id"]

    assert engine.evaluate("sensors/node1/env", '{"temp": 25}', now=1000) == []
    assert engine.evaluate("sensors/node1/env", '{"temp": 30}', now=1000) == [rule_id]
    # Still hot 10 s later: inside the 30 s cooldown, so no second publish.
    assert engine.evaluate("sensors/node1/env", '{"temp": 31}', now=1010) == []
    # After the cooldown it fires again.
    assert engine.evaluate("sensors/node1/env", '{"temp": 31}', now=1031) == [rule_id]
    assert pub.sent == [("actuators/node2/led", "on")] * 2


def test_rule_topic_wildcards(db):
    pub = FakePublisher()
    engine = RulesEngine(db, pub)
    db.create_rule({**HOT_RULE, "topic": "sensors/+/env"})
    assert engine.evaluate("sensors/node7/env", '{"temp": 40}', now=0)
    assert not engine.evaluate("sensors/node7/other", '{"temp": 40}', now=100)


def test_failed_publish_does_not_start_cooldown(db):
    pub = FakePublisher(ok=False)
    engine = RulesEngine(db, pub)
    db.create_rule(HOT_RULE)
    assert engine.evaluate("sensors/node1/env", '{"temp": 30}', now=0) == []
    pub.ok = True
    assert engine.evaluate("sensors/node1/env", '{"temp": 30}', now=1) != []


def test_disabled_rule_is_ignored(db):
    engine = RulesEngine(db, FakePublisher())
    db.create_rule({**HOT_RULE, "enabled": False})
    assert engine.evaluate("sensors/node1/env", '{"temp": 99}', now=0) == []


# --------------------------------------------------------- message routing

def make_bridge(db, pub):
    bridge = MqttBridge(db, "127.0.0.1", 1883, "test")  # never started
    bridge.publish = pub
    bridge.rules = RulesEngine(db, pub)
    return bridge


def test_reading_is_stored_and_marks_device_online(db):
    bridge = make_bridge(db, FakePublisher())
    bridge.handle_message("sensors/node1/env", '{"temp": 22.4}', retained=False, now=500)
    assert db.get_device("node1") == {"id": "node1", "status": "online", "last_seen": 500}
    assert db.latest_reading("node1")["payload"] == '{"temp": 22.4}'


def test_retained_messages_do_not_count_as_live(db):
    pub = FakePublisher()
    bridge = make_bridge(db, pub)
    db.create_rule({**HOT_RULE, "topic": "devices/+/status", "field": "x"})

    bridge.handle_message("devices/node1/status", "online", retained=False, now=100)
    # Hub restarts and the broker replays the retained status much later:
    bridge.handle_message("devices/node1/status", "online", retained=True, now=9999)
    assert db.get_device("node1")["last_seen"] == 100  # not bumped
    # Retained sensor data is not stored as a new reading either.
    bridge.handle_message("sensors/node1/env", '{"temp": 40}', retained=True, now=9999)
    assert db.latest_reading("node1") is None


def test_rules_do_not_run_on_retained_messages(db):
    pub = FakePublisher()
    bridge = make_bridge(db, pub)
    db.create_rule(HOT_RULE)
    bridge.handle_message("sensors/node1/env", '{"temp": 40}', retained=True, now=0)
    assert pub.sent == []
    bridge.handle_message("sensors/node1/env", '{"temp": 40}', retained=False, now=0)
    assert pub.sent == [("actuators/node2/led", "on")]


# ----------------------------------------------------------------- the API

class FakeBridge:
    connected = True

    def __init__(self):
        self.publish = FakePublisher()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "api.db"))
    monkeypatch.setattr(settings, "MQTT_PORT", 1)  # real bridge can't connect; harmless
    fake = FakeBridge()
    main.app.dependency_overrides[main.get_bridge] = lambda: fake
    with TestClient(main.app) as c:
        c.fake_bridge = fake
        yield c
    main.app.dependency_overrides.clear()


def test_api_devices_and_readings(client, monkeypatch):
    db = main.app.state.db
    now = main.time.time()
    db.set_device_status("node1", "online", now)
    db.insert_reading("node1", "sensors/node1/env", '{"temp": 21.0}', now - 5)
    db.insert_reading("node1", "sensors/node1/env", '{"temp": 22.0}', now)
    db.set_device_status("ghost", "online", now - 1000)  # heartbeats stopped

    devices = {d["id"]: d for d in client.get("/devices").json()}
    assert devices["node1"]["status"] == "online"
    assert devices["ghost"]["status"] == "offline"
    assert devices["ghost"]["reported_status"] == "online"

    assert client.get("/devices/node1/latest").json()["payload"] == {"temp": 22.0}
    history = client.get("/devices/node1/history?limit=1").json()
    assert [h["payload"]["temp"] for h in history] == [22.0]
    assert client.get("/devices/nope/latest").status_code == 404
    assert client.get("/devices/node1/history?limit=0").status_code == 422


def test_api_command(client):
    main.app.state.db.set_device_status("node2", "online", main.time.time())
    r = client.post("/devices/node2/command", json={"action": "on"})
    assert r.status_code == 202
    assert client.fake_bridge.publish.sent == [("actuators/node2/led", "on")]
    assert client.post("/devices/node2/command", json={"action": "blink"}).status_code == 422

    client.fake_bridge.publish.ok = False
    assert client.post("/devices/node2/command", json={"action": "off"}).status_code == 503


def test_api_rules_crud_and_validation(client):
    body = {k: v for k, v in HOT_RULE.items() if k != "enabled"}
    created = client.post("/rules", json=body)
    assert created.status_code == 201
    rule_id = created.json()["id"]
    assert [r["id"] for r in client.get("/rules").json()] == [rule_id]

    bad = [
        {**body, "operator": "~"},
        {**body, "topic": "sensors/no#de"},
        {**body, "action_topic": "actuators/+/led"},
        {**body, "topic": "#"},  # would trigger itself
    ]
    for b in bad:
        assert client.post("/rules", json=b).status_code == 422, b

    assert client.delete(f"/rules/{rule_id}").status_code == 204
    assert client.delete(f"/rules/{rule_id}").status_code == 404


def test_prune_readings(db):
    db.insert_reading("node1", "sensors/node1/env", "{}", 100)
    db.insert_reading("node1", "sensors/node1/env", "{}", 200)
    assert db.prune_readings(older_than=150) == 1
    assert [r["received_at"] for r in db.history("node1", 10)] == [200]


def test_dashboard_is_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "IoT Hub" in r.text
