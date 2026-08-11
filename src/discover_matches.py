"""Discover matches available in StatsBomb Open Data."""

from __future__ import annotations

import pandas as pd
from statsbombpy import sb


COMPETITION_ID = 2
SEASON_ID = 44
TEAM_NAME = "Arsenal"

MATCH_COLUMNS = [
    "match_id",
    "match_date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
]


def get_matches(
    competition_id: int = COMPETITION_ID,
    season_id: int = SEASON_ID,
    team_name: str | None = None,
) -> pd.DataFrame:
    """Return matches for a competition/season, optionally filtered by team."""

    matches = sb.matches(
        competition_id=competition_id,
        season_id=season_id,
    )

    missing_columns = set(MATCH_COLUMNS).difference(
        matches.columns
    )

    if missing_columns:
        raise ValueError(
            "Match data is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    matches = matches.loc[:, MATCH_COLUMNS].copy()

    if team_name:
        matches = matches.loc[
            matches["home_team"].eq(team_name)
            | matches["away_team"].eq(team_name)
        ].copy()

        if matches.empty:
            raise ValueError(
                f"No matches were found for {team_name}."
            )
    elif matches.empty:
        raise ValueError(
            "No matches were found for "
            f"competition_id={competition_id}, "
            f"season_id={season_id}."
        )

    return matches.sort_values("match_date")


def main() -> None:
    """Print available matches."""

    matches = get_matches(team_name=TEAM_NAME)

    print(
        f"\nFound {len(matches)} {TEAM_NAME} matches:\n"
    )
    print(matches.to_string(index=False))


if __name__ == "__main__":
    main()
