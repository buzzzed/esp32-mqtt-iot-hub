"""IoT hub REST API (FastAPI).

Run on the Pi with:
    uvicorn iot_hub.hub_api:app --host 0.0.0.0 --port 8000

The dashboard is at http://<pi-ip>:8000/ and interactive API docs at /docs.

Endpoints are plain `def` (not `async def`): they make blocking SQLite calls,
and FastAPI runs sync endpoints in a thread pool so they don't stall the event loop.
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse

from . import hub_settings as settings
from .hub_storage import Database
from .api_schemas import CommandIn, CommandOut, DeviceOut, ReadingOut, RuleIn, RuleOut
from .mqtt_bridge import MqttBridge

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")


log = logging.getLogger(__name__)

DASHBOARD_HTML = Path(__file__).parent / "static" / "iot_hub_dashboard.html"


async def prune_old_readings(db: Database) -> None:
    """Background task: once per interval, delete readings past retention."""
    while True:
        cutoff = time.time() - settings.READING_RETENTION_DAYS * 86400
        # SQLite calls block, so run them in a worker thread, not on the event loop.
        deleted = await asyncio.to_thread(db.prune_readings, cutoff)
        if deleted:
            log.info("pruned %d readings older than %g days",
                     deleted, settings.READING_RETENTION_DAYS)
        await asyncio.sleep(settings.PRUNE_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the database, MQTT client and pruning task with the app."""
    db = Database(settings.DB_PATH)
    bridge = MqttBridge(db, settings.MQTT_HOST, settings.MQTT_PORT, settings.MQTT_CLIENT_ID)
    bridge.start()
    pruner = None
    if settings.READING_RETENTION_DAYS > 0:
        pruner = asyncio.create_task(prune_old_readings(db))
    app.state.db = db
    app.state.bridge = bridge
    yield
    if pruner:
        pruner.cancel()
    bridge.stop()
    db.close()


app = FastAPI(title="IoT Hub", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def dashboard():
    """The web dashboard: one static page that calls the JSON API below."""
    return FileResponse(DASHBOARD_HTML)


# Dependencies: endpoints declare what they need and FastAPI injects it.
def get_db(request: Request) -> Database:
    return request.app.state.db


def get_bridge(request: Request) -> MqttBridge:
    return request.app.state.bridge


# ---------------------------------------------------------------- helpers

def to_datetime(ts: Optional[float]) -> Optional[datetime]:
    return datetime.fromtimestamp(ts, tz=timezone.utc) if ts is not None else None


def parse_payload(raw: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def device_out(row: dict, now: float) -> DeviceOut:
    """Combine what the device reported with how recently we heard from it.

    A retained "online" can be stale (e.g. the broker never delivered the
    Last Will), so "online" also requires a live message within the timeout.
    """
    reported = row["status"]
    last_seen = row["last_seen"]
    fresh = last_seen is not None and now - last_seen <= settings.OFFLINE_AFTER_SECONDS

    if reported == "online":
        effective = "online" if fresh else "offline"
    elif reported == "offline":
        effective = "offline"
    else:
        effective = "unknown"

    return DeviceOut(id=row["id"], status=effective, reported_status=reported,
                     last_seen=to_datetime(last_seen))


def reading_out(row: dict) -> ReadingOut:
    return ReadingOut(topic=row["topic"], payload=parse_payload(row["payload"]),
                      received_at=to_datetime(row["received_at"]))


def rule_out(row: dict) -> RuleOut:
    return RuleOut(**{**row, "last_fired_at": to_datetime(row["last_fired_at"])})


def require_device(db: Database, device_id: str) -> dict:
    device = db.get_device(device_id)
    if device is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown device '{device_id}'")
    return device


# ----------------------------------------------------------------- health

@app.get("/health")
def health(bridge: MqttBridge = Depends(get_bridge)):
    return {"mqtt_connected": bridge.connected}


# ---------------------------------------------------------------- devices

@app.get("/devices", response_model=list[DeviceOut])
def list_devices(db: Database = Depends(get_db)):
    now = time.time()
    return [device_out(row, now) for row in db.list_devices()]


@app.get("/devices/{device_id}/latest", response_model=ReadingOut)
def latest_reading(device_id: str, db: Database = Depends(get_db)):
    require_device(db, device_id)
    row = db.latest_reading(device_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no readings yet")
    return reading_out(row)


@app.get("/devices/{device_id}/history", response_model=list[ReadingOut])
def reading_history(device_id: str,
                    limit: int = Query(100, ge=1, le=1000),
                    db: Database = Depends(get_db)):
    """Most recent readings first."""
    require_device(db, device_id)
    return [reading_out(row) for row in db.history(device_id, limit)]


@app.post("/devices/{device_id}/command", response_model=CommandOut,
          status_code=status.HTTP_202_ACCEPTED)
def send_command(device_id: str, cmd: CommandIn,
                 db: Database = Depends(get_db),
                 bridge: MqttBridge = Depends(get_bridge)):
    """Publish a command to the device's LED actuator topic.

    202 Accepted (not 200): the broker has the message, but the device may not
    have acted yet. Confirm via actuators/<id>/led/state, which shows up in
    /devices/<id>/latest.

    Commands are NOT retained: a retained "on" would be replayed every time the
    device reconnects, possibly long after anyone wanted it.
    """
    require_device(db, device_id)
    topic = f"actuators/{device_id}/led"
    if not bridge.publish(topic, cmd.action, qos=1):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                            "not connected to MQTT broker")
    return CommandOut(topic=topic, payload=cmd.action)


# ------------------------------------------------------------------ rules

@app.get("/rules", response_model=list[RuleOut])
def list_rules(db: Database = Depends(get_db)):
    return [rule_out(r) for r in db.list_rules()]


@app.get("/rules/{rule_id}", response_model=RuleOut)
def get_rule(rule_id: int, db: Database = Depends(get_db)):
    row = db.get_rule(rule_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "rule not found")
    return rule_out(row)


@app.post("/rules", response_model=RuleOut, status_code=status.HTTP_201_CREATED)
def create_rule(rule: RuleIn, db: Database = Depends(get_db)):
    return rule_out(db.create_rule(rule.model_dump()))


@app.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_rule(rule_id: int, db: Database = Depends(get_db)):
    if not db.delete_rule(rule_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "rule not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
