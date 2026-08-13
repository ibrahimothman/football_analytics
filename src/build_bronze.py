"""Build Bronze event data from raw StatsBomb JSON."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any
from io import BytesIO

import pandas as pd

from src.config.settings import (
    BRONZE_DIR,
)
from src.storage.storage_store import (
    get_bytes,
    put_bytes,
    write_parquet,
)

logger = logging.getLogger(__name__)



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


def build_bronze(
    match_id: int,
    source: dict,
) -> str:
    """Build Bronze Parquet for latest source version."""

    raw_uri = source["raw_uri"]

    raw_content = get_bytes(
        uri=raw_uri,
    )

    raw_events = json.loads(raw_content)

    if not isinstance(raw_events, list):
        raise ValueError(
            "Expected raw events JSON to contain a list."
        )

    rows = [
        event_to_bronze_row(
            event=event,
            source=source,
        )
        for event in raw_events
    ]

    bronze_df = pd.DataFrame(rows)


    short_hash = source["file_hash"][:12]

    key = f"bronze/match_id={match_id}/events_{short_hash}.parquet"

    bronze_uri = write_parquet(
        key=key,
        df=bronze_df,
    )

    logger.info(
        "bronze_build_succeeded",
        extra={
            "source_uri": raw_uri,
            "events": len(bronze_df),
            "columns": len(bronze_df.columns),
            "output_uri": bronze_uri,
        },
    )

    return bronze_uri


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    build_bronze(args.match_id)


if __name__ == "__main__":
    main()