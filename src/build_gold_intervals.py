"""Build 5-minute Gold match interval metrics."""

from __future__ import annotations

import argparse
import logging

from src.storage.storage_store import (
    read_parquet,
    write_parquet,
)
from src.transforms.gold_intervals import silver_to_gold_intervals


logger = logging.getLogger(__name__)


def build_gold_intervals(
    match_id: int,
    silver_uri: str,
) -> str:

    silver_df = read_parquet(
        uri=silver_uri,
    )

    metrics = silver_to_gold_intervals(silver_df)

    short_hash = (
        silver_df["file_hash"]
        .iloc[0][:12]
    )

    key = (
        f"gold/match_id={match_id}/"
        f"team_intervals_{short_hash}.parquet"
    )

    gold_intervals_uri = write_parquet(
        key=key,
        df=metrics,
    )

    logger.info(
        "gold_interval_build_succeeded",
        extra={
            "intervals": len(metrics),
            "output_uri": gold_intervals_uri,
        },
    )

    return gold_intervals_uri


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    build_gold_intervals(
        args.match_id
    )


if __name__ == "__main__":
    main()
