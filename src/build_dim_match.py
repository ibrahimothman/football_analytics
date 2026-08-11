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

DIM_MATCH_FILENAME = "dim_match.parquet"
GRAIN_KEY = "match_id"

DIM_MATCH_COLUMNS = [
    "match_id",
    "match_date",
    "competition_id",
    "competition_name",
    "season_id",
    "season_name",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
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

def build_dim_match(
    competition_id: int,
    season_id: int,
) -> Path:
    """Build dim_match.parquet (1 row = 1 match)."""

    matches = fetch_matches(
        competition_id=competition_id,
        season_id=season_id,
    )
    new_df = build_dim_match_frame(matches)

    SERVING_DIR.mkdir(parents=True, exist_ok=True)
    output_path = SERVING_DIR / DIM_MATCH_FILENAME

    if output_path.exists():
        existing = pd.read_parquet(output_path)
        dim = pd.concat([existing, new_df], ignore_index=True)
        logger.info(
            "dim_match_merged_with_existing",
            extra={
                "existing_rows": len(existing),
                "incoming_rows": len(new_df),
                "merged_rows": len(dim),
            },
        )
    else:
        dim = new_df

    dim = dim.drop_duplicates(
        subset=[GRAIN_KEY],
        keep="last",
    )    


    if dim[GRAIN_KEY].duplicated().any():
        raise ValueError("Duplicate match_id rows in dim_match.")

    dim.to_parquet(output_path, index=False)

    logger.info(
        "dim_match_build_succeeded",
        extra={
            "competition_id": competition_id,
            "season_id": season_id,
            "rows": len(dim),
            "output_path": str(output_path),
        },
    )

    print(len(dim))
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build serving/dim_match.parquet from "
            "StatsBomb competition/season matches"
        ),
    )
    parser.add_argument(
        "--competition-id",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--season-id",
        type=int,
        required=True,
    )
    args = parser.parse_args()

    build_dim_match(
        competition_id=args.competition_id,
        season_id=args.season_id,
    )


if __name__ == "__main__":
    main()
