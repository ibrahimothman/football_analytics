"""Build Bronze event data from raw StatsBomb JSON."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd


logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(
    "data/metadata/ingestion_manifest.jsonl"
)

BRONZE_DIR = Path("data/bronze")


def read_manifest() -> list[dict]:
    """Read ingestion manifest records."""

    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            "Ingestion manifest does not exist. "
            "Ingest a match first."
        )

    records = []

    with MANIFEST_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        for line in file:
            if line.strip():
                records.append(json.loads(line))

    return records


def get_latest_source(
    match_id: int,
    file_hash: str,
) -> dict:
    """Return latest ingested source version for a match."""

    manifest = read_manifest()

    matches = [
        record
        for record in manifest
        if record["match_id"] == match_id and record["file_hash"] == file_hash
    ]

    if not matches:
        raise ValueError(
            f"Match {match_id} with file hash {file_hash} has not been ingested."
        )

    return max(
        matches,
        key=lambda record: record["source_version"],
    )


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
    file_hash: str,
) -> Path:
    """Build Bronze Parquet for latest source version."""

    source = get_latest_source(match_id, file_hash)

    raw_path = Path(source["raw_path"])

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw source does not exist: {raw_path}"
        )

    with raw_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        raw_events = json.load(file)

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

    match_directory = (
        BRONZE_DIR
        / f"match_id={match_id}"
    )

    match_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    short_hash = source["file_hash"][:12]

    bronze_path = (
        match_directory
        / f"events_{short_hash}.parquet"
    )

    bronze_df.to_parquet(
        bronze_path,
        index=False,
    )

    logger.info(
        "bronze_build_succeeded",
        extra={
            "source_path": str(raw_path),
            "events": len(bronze_df),
            "columns": len(bronze_df.columns),
            "output_path": str(bronze_path),
        },
    )

    return bronze_path


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