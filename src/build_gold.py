"""Build Gold team-level match metrics."""

from __future__ import annotations

import argparse
import logging

from src.storage.storage_store import (
    read_parquet,
    write_parquet,
)
from src.transforms.gold_team import silver_to_gold_team_metrics


logger = logging.getLogger(__name__)


def build_gold(
    match_id: int,
    silver_uri: str,
) -> str:

    silver_df = read_parquet(
        uri=silver_uri,
    )

    metrics = silver_to_gold_team_metrics(silver_df)

    file_hash = (
        silver_df["file_hash"]
        .iloc[0][:12]
    )

    key = (
        f"gold/match_id={match_id}/"
        f"team_metrics_{file_hash}.parquet"
    )

    gold_uri = write_parquet(
        key=key,
        df=metrics,
    )

    logger.info(
        "gold_build_succeeded",
        extra={
            "teams": len(metrics),
            "output_uri": gold_uri,
        },
    )

    return gold_uri


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    build_gold(args.match_id)


if __name__ == "__main__":
    main()
