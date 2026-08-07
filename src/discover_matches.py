"""Discover Arsenal matches available in StatsBomb Open Data."""

from __future__ import annotations

import pandas as pd
from statsbombpy import sb


COMPETITION_ID = 2
SEASON_ID = 44
TEAM_NAME = "Arsenal"


def get_matches(team_name: str, competition_id: int, season_id: int) -> pd.DataFrame:
    """Return Arsenal matches from the 2003/2004 Premier League season."""

    matches = sb.matches(
        competition_id=COMPETITION_ID,
        season_id=SEASON_ID,
    )

    required_columns = {
        "match_id",
        "match_date",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
    }

    missing_columns = required_columns.difference(matches.columns)

    if missing_columns:
        raise ValueError(
            f"Match data is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    matches = matches.loc[
        matches["home_team"].eq(team_name)
        | matches["away_team"].eq(team_name),
        [
            "match_id",
            "match_date",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
        ],
    ].copy()

    if matches.empty:
        raise ValueError(
            f"No matches were found for {team_name}."
        )

    return matches.sort_values("match_date")


def main() -> None:
    """Print available matches."""

    matches = get_matches(TEAM_NAME, COMPETITION_ID, SEASON_ID)

    print(f"\nFound {len(matches)} {TEAM_NAME} matches:\n")
    print(matches.to_string(index=False))


if __name__ == "__main__":
    main()