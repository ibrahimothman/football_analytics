"""Bronze event transforms from raw StatsBomb JSON."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


def nested_value(
    data: dict[str, Any],
    parent: str,
    child: str,
) -> Any:
    """Safely extract a value from a nested object."""

    parent_value = data.get(parent)

    if not isinstance(parent_value, dict):
        return None

    return parent_value.get(child)


def location_value(
    event: dict[str, Any],
    index: int,
) -> float | None:
    """Extract x or y from the StatsBomb location array."""

    location = event.get("location")

    if not isinstance(location, list):
        return None

    if len(location) <= index:
        return None

    return location[index]


def event_to_bronze_row(
    event: dict[str, Any],
    source: dict,
) -> dict:
    """Convert one raw StatsBomb event into a Bronze row."""

    return {
        # Source metadata
        "match_id": source["match_id"],
        "provider": source["provider"],
        "source_version": source["source_version"],
        "file_hash": source["file_hash"],
        "ingestion_id": source["ingestion_id"],
        "ingested_at": source["ingested_at"],

        # Event identity
        "event_id": event.get("id"),
        "event_index": event.get("index"),

        # Match time
        "period": event.get("period"),
        "timestamp": event.get("timestamp"),
        "minute": event.get("minute"),
        "second": event.get("second"),

        # Event type
        "event_type_id": nested_value(
            event,
            "type",
            "id",
        ),
        "event_type_name": nested_value(
            event,
            "type",
            "name",
        ),

        # Team
        "team_id": nested_value(
            event,
            "team",
            "id",
        ),
        "team_name": nested_value(
            event,
            "team",
            "name",
        ),

        # Player
        "player_id": nested_value(
            event,
            "player",
            "id",
        ),
        "player_name": nested_value(
            event,
            "player",
            "name",
        ),

        # Position
        "position_id": nested_value(
            event,
            "position",
            "id",
        ),
        "position_name": nested_value(
            event,
            "position",
            "name",
        ),

        # Possession
        "possession": event.get("possession"),
        "possession_team_id": nested_value(
            event,
            "possession_team",
            "id",
        ),
        "possession_team_name": nested_value(
            event,
            "possession_team",
            "name",
        ),

        # Play pattern
        "play_pattern_id": nested_value(
            event,
            "play_pattern",
            "id",
        ),
        "play_pattern_name": nested_value(
            event,
            "play_pattern",
            "name",
        ),

        # Raw provider coordinates
        "location_x_raw": location_value(
            event,
            0,
        ),
        "location_y_raw": location_value(
            event,
            1,
        ),

        # Other common fields
        "duration": event.get("duration"),
        "under_pressure": event.get("under_pressure"),
        "off_camera": event.get("off_camera"),
        "out": event.get("out"),

        # Nested source information
        "related_events_json": json.dumps(
            event.get("related_events"),
            ensure_ascii=False,
        ),

        # Preserve complete provider event
        "event_payload_json": json.dumps(
            event,
            ensure_ascii=False,
        ),
    }


def events_to_bronze(
    events: list[dict[str, Any]],
    source: dict,
) -> pd.DataFrame:
    """Convert a list of raw events into a Bronze DataFrame."""

    rows = [
        event_to_bronze_row(
            event=event,
            source=source,
        )
        for event in events
    ]

    return pd.DataFrame(rows)
