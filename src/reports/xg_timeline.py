"""Generate cumulative xG timeline from Silver events."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import logging

logger = logging.getLogger(__name__)

from src.config.settings import REPORTS_ROOT

from src.reports.colors import ordered_teams, team_color_map

REPORTS_DIR = REPORTS_ROOT


def prepare_xg_timeline(
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare ordered cumulative xG data."""

    shots = events[
        events["is_shot"]
    ].copy()

    if shots.empty:
        raise ValueError(
            "No shots found for xG timeline."
        )

    # Event order matters for cumulative calculations.
    shots = shots.sort_values(
        "event_index"
    )

    shots["shot_xg"] = (
        shots["shot_xg"]
        .fillna(0.0)
    )

    # A continuous match-time value.
    shots["match_minute"] = (
        shots["minute"]
        + shots["second"] / 60
    )

    shots["cumulative_xg"] = (
        shots
        .groupby("team_name")["shot_xg"]
        .cumsum()
    )

    return shots


def generate_xg_timeline(
    silver_events: pd.DataFrame,
) -> Path:
    """Generate cumulative xG timeline for both teams."""

    match_id = int(
        silver_events["match_id"].iloc[0]
    )

    shots = prepare_xg_timeline(
        silver_events
    )

    teams = list(
        shots["team_name"]
        .dropna()
        .unique()
    )

    teams = ordered_teams(teams)
    colors = team_color_map(teams)

    if len(teams) != 2:
        raise ValueError(
            "Expected exactly two teams."
        )

    fig, ax = plt.subplots(
        figsize=(12, 6)
    )

    for team in teams:

        team_shots = (
            shots[
                shots["team_name"] == team
            ]
            .sort_values("event_index")
        )

        # Start chart at 0 xG.
        x_values = [
            0.0,
            *team_shots["match_minute"].tolist(),
        ]

        y_values = [
            0.0,
            *team_shots["cumulative_xg"].tolist(),
        ]


        ax.step(
            x_values,
            y_values,
            where="post",
            linewidth=2,
            label=team,
            color=colors[team],
        )

        # Mark shot moments: hollow misses vs solid goals.
        misses = team_shots[
            team_shots["outcome"] != "Goal"
        ]
        goals = team_shots[
            team_shots["outcome"] == "Goal"
        ]

        ax.scatter(
            misses["match_minute"],
            misses["cumulative_xg"],
            s=(
                misses["shot_xg"] * 250
                + 20
            ),
            facecolors="none",
            edgecolors=colors[team],
            linewidths=1.5,
            alpha=0.6,
            zorder=3,
        )

        ax.scatter(
            goals["match_minute"],
            goals["cumulative_xg"],
            s=(
                goals["shot_xg"] * 350
                + 80
            ),
            marker="*",
            color=colors[team],
            edgecolors="black",
            linewidths=0.8,
            alpha=0.95,
            zorder=4,
        )

    ax.axvline(
        45,
        linestyle="--",
        alpha=0.5,
    )

    ax.text(
        45,
        ax.get_ylim()[1] * 0.95,
        "HT",
        ha="center",
    )

    ax.set_xlabel(
        "Match minute"
    )

    ax.set_ylabel(
        "Cumulative xG"
    )

    ax.set_title(
        f"{teams[0]} vs {teams[1]} — xG Timeline"
    )

    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.12),
        ncol=2,
        frameon=False,
    )

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.grid(
        axis="y",
        alpha=0.2,
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
        / "xg_timeline.png"
    )

    fig.savefig(
        output_path,
        dpi=150,
        bbox_inches="tight",
    )

    plt.close(fig)

    logger.info("xg_timeline_generated", extra={"match_id": match_id, "output_path": str(output_path)})

    return output_path