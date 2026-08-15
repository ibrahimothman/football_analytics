"""Build serving fact_team_match from Gold team metrics."""

from __future__ import annotations

import logging

import pandas as pd

from src.storage.storage_store import read_parquet


logger = logging.getLogger(__name__)

GRAIN_KEYS = ("match_id", "team_id")


def validate_team_match_grain(
    df: pd.DataFrame,
) -> None:
    """Ensure serving grain is unique on match_id × team_id."""

    if df.empty:
        raise ValueError(
            "Serving fact_team_match is empty."
        )

    missing = [
        column
        for column in GRAIN_KEYS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "Serving fact_team_match missing grain columns: "
            + ", ".join(missing)
        )

    duplicated = df.duplicated(
        subset=list(GRAIN_KEYS),
        keep=False,
    )

    if duplicated.any():
        dupes = (
            df.loc[duplicated, list(GRAIN_KEYS)]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        raise ValueError(
            "Duplicate (match_id, team_id) rows in "
            f"fact_team_match: {dupes}"
        )


def build_fact_team_match(gold_uri: str) -> pd.DataFrame:
    """Consolidate Gold team metrics into fact_team_match.parquet."""

    fact_df = read_parquet(
        uri=gold_uri,
    )

    validate_team_match_grain(fact_df)

    return fact_df
