"""Build serving dim_match from StatsBomb competition/season matches."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.config.settings import (
    HTTP_TIMEOUT_SECONDS,
    SERVING_DIR as SETTINGS_SERVING_DIR,
    STATSBOMB_MATCHES_URL,
)

from src.utils import nested_value


logger = logging.getLogger(__name__)

SERVING_DIR = SETTINGS_SERVING_DIR
MATCHES_URL = STATSBOMB_MATCHES_URL


COMPETITION_ID = 2
SEASON_ID = 44

DIM_MATCH_FILENAME = "dim_match.parquet"
GRAIN_KEY = "match_id"

DIM_MATCH_COLUMNS = [
    "match_id",
    "match_date",
    "kickoff_time",
    "competition_id",
    "competition_name",
    "season_id",
    "season_name",
    "match_week",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "stadium",
    "referee",
]


def fetch_matches(
    competition_id: int,
    season_id: int,
) -> list[dict[str, Any]]:
    """Fetch nested StatsBomb match objects for a competition/season."""

    url = MATCHES_URL.format(
        competition_id=competition_id,
        season_id=season_id,
    )

    response = requests.get(
        url,
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    matches = response.json()

    if not isinstance(matches, list):
        raise ValueError(
            "Expected a list of match objects from StatsBomb."
        )

    if not matches:
        raise ValueError(
            "No matches were found for "
            f"competition_id={competition_id}, "
            f"season_id={season_id}."
        )

    return matches


def match_to_dim_row(
    match: dict[str, Any],
) -> dict[str, Any]:
    """Flatten one nested StatsBomb match into a dim_match row."""

    return {
        "match_id": match.get("match_id"),
        "match_date": match.get("match_date"),
        "kickoff_time": match.get("kick_off"),
        "competition_id": nested_value(
            match,
            "competition",
            "competition_id",
        ),
        "competition_name": nested_value(
            match,
            "competition",
            "competition_name",
        ),
        "season_id": nested_value(
            match,
            "season",
            "season_id",
        ),
        "season_name": nested_value(
            match,
            "season",
            "season_name",
        ),
        "match_week": match.get("match_week"),
        "home_team_id": nested_value(
            match,
            "home_team",
            "home_team_id",
        ),
        "home_team_name": nested_value(
            match,
            "home_team",
            "home_team_name",
        ),
        "away_team_id": nested_value(
            match,
            "away_team",
            "away_team_id",
        ),
        "away_team_name": nested_value(
            match,
            "away_team",
            "away_team_name",
        ),
        "stadium": nested_value(
            match,
            "stadium",
            "name",
        ),
        "referee": nested_value(
            match,
            "referee",
            "name",
        ),
    }


def build_dim_match_frame(
    matches: list[dict[str, Any]],
) -> pd.DataFrame:
    """Convert nested match objects into a dim_match DataFrame."""

    rows = [
        match_to_dim_row(match)
        for match in matches
    ]
    return pd.DataFrame(rows, columns=DIM_MATCH_COLUMNS)


def validate_dim_match_grain(
    df: pd.DataFrame,
) -> None:
    """Ensure serving grain is unique on match_id."""

    if df.empty:
        raise ValueError("dim_match is empty.")

    missing = [
        column
        for column in DIM_MATCH_COLUMNS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "dim_match missing columns: "
            + ", ".join(missing)
        )

    if df[GRAIN_KEY].isna().any():
        raise ValueError(
            "dim_match contains null match_id values."
        )

    duplicated = df.duplicated(
        subset=[GRAIN_KEY],
        keep=False,
    )

    if duplicated.any():
        dupes = sorted(
            df.loc[duplicated, GRAIN_KEY]
            .drop_duplicates()
            .tolist()
        )
        raise ValueError(
            "Duplicate match_id rows in dim_match: "
            f"{dupes}"
        )


def build_dim_match(
    competition_id: int = COMPETITION_ID,
    season_id: int = SEASON_ID,
) -> pd.DataFrame:
    """Build dim_match.parquet (1 row = 1 match)."""

    matches = fetch_matches(
        competition_id=competition_id,
        season_id=season_id,
    )
    matches_df = build_dim_match_frame(matches)

    validate_dim_match_grain(matches_df)
    return matches_df
