"""Build canonical Silver football events."""

from __future__ import annotations

import argparse
import logging

import json

import pyarrow as pa

from src.config.settings import MODELS_ROOT

from src.storage.iceberg import load_table
from src.storage.storage_store import (
    read_parquet,
)
from src.transforms.silver import bronze_to_silver

from openlineage.client.event_v2 import (
    InputDataset,
    OutputDataset,
)

from airflow.providers.openlineage.api.datasets import (
    emit_dataset_lineage,
)

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


def load_into_silver_table(
    match_id: int,
    arrow_table: pa.Table,
) -> dict:
   
    table_name = (
        "football.silver_events"
    )

    table = load_table(
        table_name=table_name,
        schema=arrow_table.schema,
    )

    table.overwrite(
        arrow_table,
        overwrite_filter=f"match_id = {match_id}",
        snapshot_properties={
            "match_id": str(match_id),
        },
    )

    return {
        "table_name": table_name,
        "match_id": match_id,
        "snapshot_id": table.current_snapshot().snapshot_id,
    }


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

    result = load_into_silver_table(
        match_id=match_id,
        arrow_table=pa.Table.from_pandas(silver_df),
    )

    emit_dataset_lineage(
        inputs=[
            InputDataset(
                namespace="file",
                name=str(bronze_uri),
                facets={},
                inputFacets={},
            )
        ],
        outputs=[
            OutputDataset(
                namespace="iceberg",
                name="football.silver_events",
                facets={},
                outputFacets={},
            )
        ],
    )

    return result


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
