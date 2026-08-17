"""Expected Threat model utilities."""

from __future__ import annotations

import pandas as pd

XT_MODEL_VERSION = "open_xt_12x8_v1"

def continuous_to_cell(
    point: tuple[float, float],
    continuous_size: tuple[float, float],
    discrete_size: tuple[int, int],
) -> tuple[int, int]:
    """Convert continuous pitch coordinates into discrete grid indexes."""

    x, y = point
    x_max, y_max = continuous_size
    n_x, n_y = discrete_size

    col = int(x / x_max * n_x)
    row = int(y / y_max * n_y)

    # Protect boundries
    col = min(max(col, 0), n_x - 1)
    row = min(max(row, 0), n_y - 1)

    return col, row



def get_xt_value(
    grid: list[list[float]],
    pitch_size: tuple[float, float],
    x: float | None,
    y: float | None,
) -> float | None:
    """Return xT value for a pitch location."""

    if pd.isna(x) or pd.isna(y):
        return None

    col, row = (
        continuous_to_cell(
            point=(x, y),
            continuous_size=pitch_size,
            discrete_size=(len(grid[0]), len(grid)),

        )
    )

    return float(
        grid[row][col]
    )    


def rate_move(
    grid: list[list[float]],
    pitch_size: tuple[float, float],
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
        pitch_size,
        start_x,
        start_y,
    )

    xt_end = get_xt_value(
        grid,
        pitch_size,
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