"""Rules engine: "when <field> on <topic> <operator> <value>, publish <action>".

Every live (non-retained) MQTT message the hub receives is passed to
RulesEngine.evaluate(). For each enabled rule we check, in order:

  1. does the message topic match the rule's topic filter (wildcards allowed)?
  2. does the JSON payload contain the rule's field as a number?
  3. does `actual <operator> value` hold?
  4. has at least cooldown_seconds passed since the rule last fired?

If all four pass, we publish action_payload to action_topic and record the time.

Threading: evaluate() runs on paho's single network thread, so messages are
processed one at a time and the cooldown check-then-update can't race.
"""

import json
import logging
import operator
import time
from typing import Any, Callable, Optional

from paho.mqtt.client import topic_matches_sub

log = logging.getLogger(__name__)

# Map the operator strings stored in rules to Python's comparison functions.
OPERATORS: dict[str, Callable[[float, float], bool]] = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,  # exact float equality: best used with integer values
    "!=": operator.ne,
}


def extract_field(payload: str, field: str) -> Optional[float]:
    """Return the numeric value of `field` in a JSON payload, or None.

    Dotted fields reach into nested objects: "env.temp" -> {"env": {"temp": 1}}.
    Non-JSON payloads, missing fields and non-numeric values all return None,
    which simply means "this rule doesn't apply to this message".
    """
    try:
        data: Any = json.loads(payload)
    except ValueError:
        return None

    for key in field.split("."):
        if not isinstance(data, dict) or key not in data:
            return None
        data = data[key]

    # bool is a subclass of int in Python; we don't want True to compare as 1.
    if isinstance(data, bool) or not isinstance(data, (int, float)):
        return None
    return float(data)


class RulesEngine:
    def __init__(self, db, publish: Callable[[str, str], bool]):
        """
        db:      the Database (rules are read from it on every message)
        publish: function(topic, payload) -> True if the message was queued
        """
        self._db = db
        self._publish = publish

    def evaluate(self, topic: str, payload: str,
                 now: Optional[float] = None) -> list[int]:
        """Check one incoming message against all rules; return IDs that fired."""
        now = time.time() if now is None else now
        fired: list[int] = []

        # Reading rules from SQLite per message keeps things simple: rules
        # created/deleted via the API take effect immediately with no cache
        # invalidation. At a few messages per second this costs ~nothing.
        for rule in self._db.list_rules(enabled_only=True):
            if not topic_matches_sub(rule["topic"], topic):
                continue

            actual = extract_field(payload, rule["field"])
            if actual is None:
                continue

            if not OPERATORS[rule["operator"]](actual, rule["value"]):
                continue

            last = rule["last_fired_at"]
            if last is not None and now - last < rule["cooldown_seconds"]:
                continue  # condition holds, but we fired too recently

            # Only record the firing if the publish actually went out; if the
            # broker is unreachable, the next matching message will retry.
            if self._publish(rule["action_topic"], rule["action_payload"]):
                self._db.mark_rule_fired(rule["id"], now)
                fired.append(rule["id"])
                log.info("rule %d (%s) fired: %s=%s %s %s -> %s %r",
                         rule["id"], rule["name"], rule["field"], actual,
                         rule["operator"], rule["value"],
                         rule["action_topic"], rule["action_payload"])
            else:
                log.warning("rule %d (%s) matched but publish failed",
                            rule["id"], rule["name"])

        return fired
