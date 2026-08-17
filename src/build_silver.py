"""Build canonical Silver football events."""

from __future__ import annotations

import argparse
import logging

import json

from src.storage.storage_store import (
    read_parquet,
    write_parquet,
)
from src.transforms.silver import bronze_to_silver
from src.config.settings import MODELS_ROOT

MODEL_PATH = MODELS_ROOT / "xt" / "open_xt_12x8_v1.json"

logger = logging.getLogger(__name__)

PITCH_LENGTH = 105
PITCH_WIDTH = 68

GRID_COLUMNS = 12
GRID_ROWS = 8


def load_xt_grid() -> list[list[float]]:
    """Load and validate the xT grid."""

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            "xT model not found. "
            "Run src.download_xt_model first."
        )

    with MODEL_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        grid = json.load(file)

    if len(grid) != GRID_ROWS:
        raise ValueError(
            "Invalid xT grid row count."
        )

    if any(
        len(row) != GRID_COLUMNS
        for row in grid
    ):
        raise ValueError(
            "Invalid xT grid column count."
        )

    return grid
    
def build_silver(
    match_id: int,
    bronze_uri: str,
) -> str:

    bronze_df = read_parquet(
        uri=bronze_uri,
    )

    xt_grid = load_xt_grid()
    pitch_size = (PITCH_LENGTH, PITCH_WIDTH)
    silver_df = bronze_to_silver(bronze_df, xt_grid, pitch_size)

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
