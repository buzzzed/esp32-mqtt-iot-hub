"""SQLite storage for devices, readings and rules.

Concurrency model: two kinds of threads touch the database:
  * paho-mqtt's network thread (writes readings/status as messages arrive), and
  * FastAPI's worker threads (serve HTTP requests).
We share ONE connection between them and serialise access with a lock. For a
hub handling a handful of messages per second that is simple and plenty fast;
WAL mode keeps writes cheap on the Pi's SD card.

All timestamps are stored as Unix epoch seconds (REAL); the API layer converts
them to ISO-8601 for clients.
"""

import sqlite3
import threading
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    id         TEXT PRIMARY KEY,
    status     TEXT NOT NULL DEFAULT 'unknown',  -- last status the device reported
    last_seen  REAL                              -- last time we heard from it live
);

CREATE TABLE IF NOT EXISTS readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT NOT NULL,
    topic       TEXT NOT NULL,
    payload     TEXT NOT NULL,                   -- raw payload, usually JSON
    received_at REAL NOT NULL
);
-- "latest" and "history" both query by device, newest first.
CREATE INDEX IF NOT EXISTS idx_readings_device_time
    ON readings (device_id, received_at DESC);

CREATE TABLE IF NOT EXISTS rules (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL,
    topic            TEXT NOT NULL,              -- MQTT topic filter, wildcards allowed
    field            TEXT NOT NULL,              -- JSON field, dots for nesting
    operator         TEXT NOT NULL,
    value            REAL NOT NULL,
    action_topic     TEXT NOT NULL,
    action_payload   TEXT NOT NULL,
    cooldown_seconds INTEGER NOT NULL DEFAULT 0,
    enabled          INTEGER NOT NULL DEFAULT 1,
    last_fired_at    REAL
);
"""

RULE_FIELDS = (
    "name", "topic", "field", "operator", "value",
    "action_topic", "action_payload", "cooldown_seconds", "enabled",
)


class Database:
    def __init__(self, path: str):
        # check_same_thread=False: we deliberately share this connection across
        # threads, and self._lock makes that safe.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row  # rows behave like dicts
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # Small helpers: every query takes the lock; `with self._conn` commits on
    # success and rolls back on exception.
    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock, self._conn:
            return self._conn.execute(sql, params)

    def _fetchall(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def _fetchone(self, sql: str, params: tuple = ()) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(sql, params).fetchone()
            return dict(row) if row else None

    # ----------------------------------------------------------------- devices

    def set_device_status(self, device_id: str, status: str,
                          seen_at: Optional[float]) -> None:
        """Create or update a device.

        seen_at is None for *retained* messages: those are replays of old
        state, not proof the device is alive right now, so they must not bump
        last_seen (COALESCE keeps the previous value).
        """
        self._execute(
            """
            INSERT INTO devices (id, status, last_seen) VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                status    = excluded.status,
                last_seen = COALESCE(excluded.last_seen, devices.last_seen)
            """,
            (device_id, status, seen_at),
        )

    def list_devices(self) -> list[dict[str, Any]]:
        return self._fetchall("SELECT * FROM devices ORDER BY id")

    def get_device(self, device_id: str) -> Optional[dict[str, Any]]:
        return self._fetchone("SELECT * FROM devices WHERE id = ?", (device_id,))

    # ---------------------------------------------------------------- readings

    def insert_reading(self, device_id: str, topic: str, payload: str,
                       received_at: float) -> None:
        self._execute(
            "INSERT INTO readings (device_id, topic, payload, received_at) "
            "VALUES (?, ?, ?, ?)",
            (device_id, topic, payload, received_at),
        )

    def history(self, device_id: str, limit: int) -> list[dict[str, Any]]:
        """Most recent readings first."""
        return self._fetchall(
            "SELECT topic, payload, received_at FROM readings "
            "WHERE device_id = ? ORDER BY received_at DESC, id DESC LIMIT ?",
            (device_id, limit),
        )

    def latest_reading(self, device_id: str) -> Optional[dict[str, Any]]:
        rows = self.history(device_id, 1)
        return rows[0] if rows else None

    def prune_readings(self, older_than: float) -> int:
        """Delete readings received before `older_than`; return how many."""
        return self._execute("DELETE FROM readings WHERE received_at < ?",
                             (older_than,)).rowcount

    # ------------------------------------------------------------------- rules

    def list_rules(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM rules"
        if enabled_only:
            sql += " WHERE enabled = 1"
        return [_rule_row(r) for r in self._fetchall(sql + " ORDER BY id")]

    def get_rule(self, rule_id: int) -> Optional[dict[str, Any]]:
        row = self._fetchone("SELECT * FROM rules WHERE id = ?", (rule_id,))
        return _rule_row(row) if row else None

    def create_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        cols = ", ".join(RULE_FIELDS)
        placeholders = ", ".join("?" for _ in RULE_FIELDS)
        cur = self._execute(
            f"INSERT INTO rules ({cols}) VALUES ({placeholders})",
            tuple(rule[f] for f in RULE_FIELDS),
        )
        return self.get_rule(cur.lastrowid)

    def delete_rule(self, rule_id: int) -> bool:
        return self._execute("DELETE FROM rules WHERE id = ?", (rule_id,)).rowcount > 0

    def mark_rule_fired(self, rule_id: int, fired_at: float) -> None:
        self._execute("UPDATE rules SET last_fired_at = ? WHERE id = ?",
                      (fired_at, rule_id))


def _rule_row(row: dict[str, Any]) -> dict[str, Any]:
    row["enabled"] = bool(row["enabled"])  # SQLite has no boolean type
    return row
