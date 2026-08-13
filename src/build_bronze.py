"""Build Bronze event data from raw StatsBomb JSON."""

from __future__ import annotations

import argparse
import json
import logging

from src.storage.storage_store import (
    get_bytes,
    write_parquet,
)
from src.transforms.bronze import events_to_bronze

logger = logging.getLogger(__name__)


def build_bronze(
    match_id: int,
    source: dict,
) -> str:
    """Build Bronze Parquet for latest source version."""

    raw_content = get_bytes(
        uri=source["raw_uri"],
    )

    raw_events = json.loads(raw_content)

    bronze_df = events_to_bronze(raw_events, source)

    short_hash = source["file_hash"][:12]

    key = f"bronze/match_id={match_id}/events_{short_hash}.parquet"

    bronze_uri = write_parquet(
        key=key,
        df=bronze_df,
    )

    logger.info(
        "bronze_build_succeeded",
        extra={
            "source_uri": source["raw_uri"],
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
