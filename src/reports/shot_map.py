"""Generate analyst-style shot maps from Silver events."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from mplsoccer import VerticalPitch

import logging

logger = logging.getLogger(__name__)

from src.config.settings import REPORTS_ROOT


REPORTS_DIR = REPORTS_ROOT


def _marker_sizes(
    shot_xg: pd.Series,
) -> pd.Series:
    """Convert xG into plot marker sizes."""

    return (
        shot_xg.fillna(0.05) * 1200
        + 60
    )


def _plot_team_shots(
    ax,
    team_shots: pd.DataFrame,
    team_name: str,
) -> None:
    """Plot one team's shots on a half pitch."""

    pitch = VerticalPitch(
        pitch_type="custom",
        pitch_length=105,
        pitch_width=68,
        half=True,
        line_zorder=2,
    )

    pitch.draw(ax=ax)

    goals = team_shots[
        team_shots["outcome"] == "Goal"
    ]

    non_goals = team_shots[
        team_shots["outcome"] != "Goal"
    ]

    # Non-goal shots
    pitch.scatter(
        non_goals["start_x"],
        non_goals["start_y"],
        s=_marker_sizes(
            non_goals["shot_xg"]
        ),
        alpha=0.7,
        ax=ax,
        label="Shot",
    )

    # Goals
    pitch.scatter(
        goals["start_x"],
        goals["start_y"],
        s=_marker_sizes(
            goals["shot_xg"]
        ),
        marker="*",
        alpha=0.9,
        ax=ax,
        label="Goal",
    )

    total_shots = len(team_shots)
    goals_count = len(goals)
    total_xg = (
        team_shots["shot_xg"]
        .fillna(0)
        .sum()
    )

    ax.set_title(
        (
            f"{team_name}\n"
            f"Shots: {total_shots} | "
            f"Goals: {goals_count} | "
            f"xG: {total_xg:.2f}"
        ),
        fontsize=13,
    )

    ax.legend(
        loc="upper left",
        fontsize=9,
    )


def generate_shot_map(
    silver_path: Path,
) -> Path:
    """Generate analyst-style two-panel shot map."""

    df = pd.read_parquet(
        silver_path
    )

    shots = df[
        df["is_shot"]
    ].copy()

    if shots.empty:
        raise ValueError(
            "No shots found in Silver data."
        )

    match_id = int(
        df["match_id"].iloc[0]
    )

    teams = list(
        shots["team_name"]
        .dropna()
        .unique()
    )

    if len(teams) != 2:
        raise ValueError(
            "Expected exactly two teams."
        )

    fig, axes = plt.subplots(
        nrows=1,
        ncols=2,
        figsize=(10, 12),
    )

    for ax, team_name in zip(axes, teams):
        team_shots = shots[
            shots["team_name"] == team_name
        ].copy()

        _plot_team_shots(
            ax=ax,
            team_shots=team_shots,
            team_name=team_name,
        )

    fig.suptitle(
        f"{teams[0]} vs {teams[1]} — Shot Map",
        fontsize=18,
        y=0.98,
    )

    report_directory = (
        REPORTS_DIR
        / f"match_id={match_id}"
    )

    report_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        report_directory
        / "shots.png"
    )

    fig.savefig(
        output_path,
        dpi=150,
        bbox_inches="tight",
    )

    plt.close(fig)

    logger.info("shot_map_generated", extra={"match_id": match_id, "output_path": str(output_path)})
    return output_path