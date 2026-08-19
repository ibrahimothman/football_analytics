"""Generate 5-minute xT threat momentum chart."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import logging

logger = logging.getLogger(__name__)

from src.config.settings import REPORTS_ROOT

from src.reports.colors import ordered_teams, team_color_map


REPORTS_DIR = REPORTS_ROOT


INTERVAL_ORDER = [
    "0-5",
    "5-10",
    "10-15",
    "15-20",
    "20-25",
    "25-30",
    "30-35",
    "35-40",
    "40-45",
    "45+",
    "45-50",
    "50-55",
    "55-60",
    "60-65",
    "65-70",
    "70-75",
    "75-80",
    "80-85",
    "85-90",
    "90+",
]


def generate_xt_momentum(
    gold_intervals_metrics: pd.DataFrame,
) -> Path:
    """Generate xT threat momentum chart from Gold interval metrics."""

    if gold_intervals_metrics.empty:
        raise ValueError(
            "Gold interval dataset is empty."
        )

    match_id = int(
        gold_intervals_metrics["match_id"].iloc[0]
    )

    teams = list(
        gold_intervals_metrics["team_name"]
        .dropna()
        .unique()
    )

    teams = ordered_teams(teams)
    colors = team_color_map(teams)

    if len(teams) != 2:
        raise ValueError(
            "Expected exactly two teams."
        )

    # Map each football interval to a stable display order.
    order_lookup = {
        label: index
        for index, label in enumerate(
            INTERVAL_ORDER
        )
    }

    gold_intervals_metrics = gold_intervals_metrics.copy()

    gold_intervals_metrics["display_order"] = (
        gold_intervals_metrics["interval_label"]
        .map(order_lookup)
    )

    unknown_intervals = gold_intervals_metrics[
        gold_intervals_metrics["display_order"].isna()
    ]

    if not unknown_intervals.empty:
        raise ValueError(
            "Unexpected interval labels found: "
            f"{unknown_intervals['interval_label'].unique().tolist()}"
        )

    # Build one aligned row per expected interval.
    team_a = (
        gold_intervals_metrics[
            gold_intervals_metrics["team_name"] == teams[0]
        ]
        .set_index("interval_label")
        .reindex(INTERVAL_ORDER)
    )

    team_b = (
        gold_intervals_metrics[
            gold_intervals_metrics["team_name"] == teams[1]
        ]
        .set_index("interval_label")
        .reindex(INTERVAL_ORDER)
    )

    # Densification should already guarantee these exist,
    # but fail loudly if something unexpected happened.
    if team_a["positive_xt"].isna().any():
        raise ValueError(
            f"Missing xT intervals for {teams[0]}."
        )

    if team_b["positive_xt"].isna().any():
        raise ValueError(
            f"Missing xT intervals for {teams[1]}."
        )

    x_positions = list(
        range(len(INTERVAL_ORDER))
    )

    fig, ax = plt.subplots(
        figsize=(15, 7)
    )

    # Team A is plotted upward.
    ax.bar(
        x_positions,
        team_a["positive_xt"],
        width=0.8,
        label=teams[0],
        color=colors[teams[0]],
    )

    # Team B has positive xT too.
    # We multiply by -1 only for visual comparison.
    ax.bar(
        x_positions,
        -team_b["positive_xt"],
        width=0.8,
        label=teams[1],
        color=colors[teams[1]],
    )

    # Zero baseline.
    ax.axhline(
        0,
        linewidth=1,
    )

    # Halftime falls between 45+ and 45-50.
    halftime_position = 9.5

    ax.axvline(
        halftime_position,
        linestyle="--",
        alpha=0.5,
    )

    # Add HT label near the top of the chart.
    y_min, y_max = ax.get_ylim()

    ax.text(
        halftime_position,
        y_max * 0.95,
        "HT",
        ha="center",
        va="top",
    )

    ax.set_xticks(
        x_positions
    )

    ax.set_xticklabels(
        INTERVAL_ORDER,
        rotation=45,
        ha="right",
    )

    ax.set_xlabel(
        "Match interval"
    )

    ax.set_ylabel(
        "Positive xT generated"
    )

    ax.set_title(
        f"{teams[0]} vs {teams[1]} — xT Threat Momentum"
    )

    ax.legend()

    ax.grid(
        axis="y",
        alpha=0.2,
    )

    # Optional explanatory note because the second
    # team's bars are visually inverted.
    ax.text(
        0.01,
        0.02,
        (
            f"{teams[0]} shown above zero; "
            f"{teams[1]} shown below zero "
            "for visual comparison only."
        ),
        transform=ax.transAxes,
        fontsize=9,
        alpha=0.7,
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
        / "xt_momentum.png"
    )

    fig.savefig(
        output_path,
        dpi=150,
        bbox_inches="tight",
    )

    plt.close(fig)

    logger.info("xt_momentum_generated", extra={"match_id": match_id, "output_path": str(output_path)})

    return output_path