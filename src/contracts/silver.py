from __future__ import annotations

import pandas as pd

from pandas.api.types import (
    is_bool_dtype,
    is_integer_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)


REQUIRED_SILVER_COLUMNS: dict[str, str] = {
    "match_id": "integer",
    "event_id": "string",
    "event_index": "integer",
    "period": "integer",
    "minute": "integer",
    "second": "integer",
    "event_type": "string",
    "team_id": "integer",
    "team_name": "string",
    "player_id": "integer",
    "player_name": "string",
    "possession_id": "integer",
    "start_x": "numeric",
    "start_y": "numeric",
    "end_x": "numeric",
    "end_y": "numeric",
    "outcome": "string",
    "shot_xg": "numeric",
    "is_pass": "boolean",
    "is_carry": "boolean",
    "is_shot": "boolean",
    "is_completed_pass": "boolean",
    "is_progressive_pass": "boolean",
    "progress_ratio": "numeric",
    "progress_toward_goal_m": "numeric",
    "is_successful_move": "boolean",
    "xt_start": "numeric",
    "xt_end": "numeric",
    "xt_added": "numeric",
    "xt_model_version": "string",
    "source_version": "integer",
    "file_hash": "string",
}


def validate_silver_schema(df: pd.DataFrame) -> None:
    missing_columns = (
        REQUIRED_SILVER_COLUMNS.keys()
        - set(df.columns)
    )
    if missing_columns:
        raise ValueError(
            "Silver schema contract violated. "
            f"Missing columns: "
            f"{sorted(missing_columns)}"
        )

def _match_type(series: pd.Series, expected_type: str) -> bool:

    dtype = series.dtype

    if expected_type == "numeric":
        return is_numeric_dtype(dtype)
    if expected_type == "integer":
        return is_integer_dtype(dtype)
    if expected_type == "boolean":
        return is_bool_dtype(dtype)
    if expected_type == "string":
        return is_string_dtype(dtype)

    raise ValueError(
        f"Unknown type rule: {expected_type}"
    )


def validate_silver_type(df: pd.DataFrame) -> None:
    errors = []
    for column, expected_type in REQUIRED_SILVER_COLUMNS.items():
        if column not in df.columns:
            continue
        if not _match_type(df[column], expected_type):
            errors.append(
                f"{column}: "
                f"expected {expected_type}, "
                f"got {df[column].dtype}"
            )
    if errors:
        raise ValueError(
            "Silver type contract violated. "
            f"Errors: "
            f"{sorted(errors)}"
        )