"""Build Gold team-level match metrics."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from src.quality.reconcilation import reconcile_silver_to_gold_team


logger = logging.getLogger(__name__)

GOLD_DIR = Path("data/gold")

METRIC_VERSION = "1.0"


def calculate_team_metrics(
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate event-level Silver data to team-match grain."""

    rows = []

    teams = (
        events[
            ["team_id", "team_name"]
        ]
        .dropna(subset=["team_id"])
        .drop_duplicates()
    )

    for _, team in teams.iterrows():

        team_events = events[
            events["team_id"] == team["team_id"]
        ]

        shots = int(
            team_events["is_shot"].sum()
        )

        goals = int(
            (
                team_events["is_shot"]
                & team_events["outcome"].eq("Goal")
            ).sum()
        )

        xg = float(
            team_events.loc[
                team_events["is_shot"],
                "shot_xg",
            ]
            .fillna(0)
            .sum()
        )

        passes_attempted = int(
            team_events["is_pass"].sum()
        )

        passes_completed = int(
            team_events["is_completed_pass"].sum()
        )

        progressive_passes = int(
            team_events["is_progressive_pass"].sum()
        )

        carries = int(
            team_events["is_carry"].sum()
        )

        pass_completion_pct = (
            passes_completed
            / passes_attempted
            * 100
            if passes_attempted > 0
            else None
        )

        rows.append(
            {
                "match_id": team_events[
                    "match_id"
                ].iloc[0],

                "team_id": int(
                    team["team_id"]
                ),

                "team_name": team[
                    "team_name"
                ],

                "shots": shots,
                "goals": goals,
                "xg": xg,

                "passes_attempted": passes_attempted,
                "passes_completed": passes_completed,
                "pass_completion_pct": pass_completion_pct,

                "progressive_passes": progressive_passes,

                "carries": carries,

                "source_version": team_events[
                    "source_version"
                ].iloc[0],

                "file_hash": team_events[
                    "file_hash"
                ].iloc[0],

                "metric_version": METRIC_VERSION,
            }
        )

    return pd.DataFrame(rows)


def run_gold_dq_checks(
    df: pd.DataFrame,
) -> None:
    """Validate team-match metrics."""

    if df.empty:
        raise ValueError(
            "Gold dataset is empty."
        )

    if len(df) != 2:
        raise ValueError(
            "Expected exactly two teams in match."
        )

    if (df["goals"] > df["shots"]).any():
        raise ValueError(
            "Goals cannot exceed shots."
        )

    if (
        df["passes_completed"]
        > df["passes_attempted"]
    ).any():
        raise ValueError(
            "Completed passes exceed attempted passes."
        )

    if (
        df["progressive_passes"]
        > df["passes_completed"]
    ).any():
        raise ValueError(
            "Progressive passes exceed completed passes."
        )

    invalid_pct = (
        df["pass_completion_pct"]
        .dropna()
        .between(0, 100)
        == False
    )

    if invalid_pct.any():
        raise ValueError(
            "Invalid pass completion percentage."
        )

    if (df["xg"] < 0).any():
        raise ValueError(
            "xG cannot be negative."
        )    


def build_gold(
    match_id: int,
    silver_path: Path,
) -> Path:

    if not silver_path.exists():
        raise FileNotFoundError(
            f"Silver artifact not found at {silver_path}"
        )

    events = pd.read_parquet(
        silver_path
    )

    metrics = calculate_team_metrics(
        events
    )


    run_gold_dq_checks(metrics)
    reconcile_silver_to_gold_team(events, metrics)

    match_directory = (
        GOLD_DIR
        / f"match_id={match_id}"
    )

    match_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_hash = (
        events["file_hash"]
        .iloc[0][:12]
    )

    gold_path = (
        match_directory
        / f"team_metrics_{file_hash}.parquet"
    )

    metrics.to_parquet(
        gold_path,
        index=False,
    )

    logger.info(
        "gold_build_succeeded",
        extra={
            "teams": len(metrics),
            "output_path": str(gold_path),
        },
    )

    return gold_path        


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    build_gold(args.match_id)


if __name__ == "__main__":
    main()    