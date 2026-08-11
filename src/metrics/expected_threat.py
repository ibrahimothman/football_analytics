"""Expected Threat model utilities."""

from __future__ import annotations

import json

import pandas as pd

from src.config import MODELS_ROOT


MODEL_PATH = MODELS_ROOT / "xt" / "open_xt_12x8_v1.json"

XT_MODEL_VERSION = "open_xt_12x8_v1"

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

def coordinate_to_cell(
    x: float,
    y: float,
) -> tuple[int, int]:
    """Convert pitch coordinates into xT grid indexes."""

    x_index = int(
        x / PITCH_LENGTH
        * GRID_COLUMNS
    )

    y_index = int(
        y / PITCH_WIDTH
        * GRID_ROWS
    )

    # Protect pitch-boundary values such as x=105.
    x_index = min(
        max(x_index, 0),
        GRID_COLUMNS - 1,
    )

    y_index = min(
        max(y_index, 0),
        GRID_ROWS - 1,
    )

    return x_index, y_index    


def get_xt_value(
    grid: list[list[float]],
    x: float | None,
    y: float | None,
) -> float | None:
    """Return xT value for a pitch location."""

    if pd.isna(x) or pd.isna(y):
        return None

    x_index, y_index = (
        coordinate_to_cell(
            x=x,
            y=y,
        )
    )

    return float(
        grid[y_index][x_index]
    )    


def rate_move(
    grid: list[list[float]],
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
) -> tuple[
    float | None,
    float | None,
    float | None,
]:
    """Calculate xT added by one successful move."""

    xt_start = get_xt_value(
        grid,
        start_x,
        start_y,
    )

    xt_end = get_xt_value(
        grid,
        end_x,
        end_y,
    )

    if (
        xt_start is None
        or xt_end is None
    ):
        return None, None, None

    xt_added = (
        xt_end - xt_start
    )

    return (
        xt_start,
        xt_end,
        xt_added,
    )    