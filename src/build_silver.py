"""Build canonical Silver football events."""

from __future__ import annotations

import argparse
import logging

from src.metrics.expected_threat import load_xt_grid
from src.storage.storage_store import (
    read_parquet,
    write_parquet,
)
from src.transforms.silver import bronze_to_silver


logger = logging.getLogger(__name__)


def build_silver(
    match_id: int,
    bronze_uri: str,
) -> str:

    bronze_df = read_parquet(
        uri=bronze_uri,
    )

    xt_grid = load_xt_grid()
    silver_df = bronze_to_silver(bronze_df, xt_grid)

    file_hash = (
        silver_df["file_hash"]
        .iloc[0]
    )

    key = f"silver/match_id={match_id}/events_{file_hash[:12]}.parquet"

    silver_uri = write_parquet(
        key=key,
        df=silver_df,
    )

    logger.info(
        "silver_build_succeeded",
        extra={
            "rows_out": len(silver_df),
            "output_uri": silver_uri,
        },
    )

    return silver_uri


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    build_silver(args.match_id)


if __name__ == "__main__":
    main()
