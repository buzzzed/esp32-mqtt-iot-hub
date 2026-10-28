"""Pydantic models: the shapes of API request and response bodies.

FastAPI uses these to validate input (bad requests get an automatic 422 with
a helpful error) and to generate the interactive docs at /docs.
"""

from datetime import datetime
from typing import Any, Literal, Optional

from paho.mqtt.client import topic_matches_sub
from pydantic import BaseModel, Field, field_validator, model_validator

Operator = Literal[">", ">=", "<", "<=", "==", "!="]


class DeviceOut(BaseModel):
    id: str
    status: Literal["online", "offline", "unknown"]  # what the hub believes
    reported_status: str                              # what the device last said
    last_seen: Optional[datetime]


class ReadingOut(BaseModel):
    topic: str
    payload: Any  # parsed JSON when possible, otherwise the raw string
    received_at: datetime


class CommandIn(BaseModel):
    action: Literal["on", "off"]


class CommandOut(BaseModel):
    topic: str
    payload: str


class RuleIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    topic: str = Field(description="MQTT topic filter; + and # wildcards allowed")
    field: str = Field(min_length=1, description='JSON field, e.g. "temp" or "env.temp"')
    operator: Operator
    value: float
    action_topic: str
    action_payload: str
    cooldown_seconds: int = Field(default=0, ge=0)
    enabled: bool = True

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "fan on when hot",
                "topic": "sensors/node1/env",
                "field": "temp",
                "operator": ">",
                "value": 28,
                "action_topic": "actuators/node2/led",
                "action_payload": "on",
                "cooldown_seconds": 30,
            }
        }
    }

    @field_validator("topic")
    @classmethod
    def valid_topic_filter(cls, v: str) -> str:
        # MQTT 3.1.1 rules: '+' must be a whole level; '#' must be a whole
        # level AND the last one.
        if not v:
            raise ValueError("topic must not be empty")
        levels = v.split("/")
        for i, level in enumerate(levels):
            if "+" in level and level != "+":
                raise ValueError("'+' must occupy an entire topic level")
            if "#" in level and (level != "#" or i != len(levels) - 1):
                raise ValueError("'#' must be the last topic level on its own")
        return v

    @field_validator("action_topic")
    @classmethod
    def no_wildcards_in_action(cls, v: str) -> str:
        if not v or "+" in v or "#" in v:
            raise ValueError("action_topic must be a concrete topic (no wildcards)")
        return v

    @model_validator(mode="after")
    def no_self_trigger(self) -> "RuleIn":
        # A rule whose action is published onto a topic the rule itself watches
        # would feed on its own output. (Longer cycles across several rules are
        # still possible; the cooldown bounds how fast they can spin.)
        if topic_matches_sub(self.topic, self.action_topic):
            raise ValueError("action_topic matches the rule's own topic filter")
        return self


class RuleOut(RuleIn):
    id: int
    last_fired_at: Optional[datetime]
