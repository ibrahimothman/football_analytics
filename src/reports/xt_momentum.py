"""Generate 5-minute xT threat momentum chart."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REPORTS_DIR = Path("reports")


def generate_xt_momentum(
    gold_intervals_path: Path,
) -> Path:

    df = pd.read_parquet(
        gold_intervals_path
    )

    match_id = int(
        df["match_id"].iloc[0]
    )

    teams = list(
        df["team_name"]
        .dropna()
        .unique()
    )

    if len(teams) != 2:
        raise ValueError(
            "Expected exactly two teams."
        )

    team_a = (
        df[
            df["team_name"] == teams[0]
        ]
        .sort_values("interval_start")
    )

    team_b = (
        df[
            df["team_name"] == teams[1]
        ]
        .sort_values("interval_start")
    )

    fig, ax = plt.subplots(
        figsize=(13, 6)
    )

    x_a = (
        team_a["interval_start"]
        + 2.5
    )

    x_b = (
        team_b["interval_start"]
        + 2.5
    )

    # Team A is displayed upward.
    ax.bar(
        x_a,
        team_a["positive_xt"],
        width=4.2,
        label=teams[0],
    )

    # Team B is displayed downward purely
    # for visual comparison.
    ax.bar(
        x_b,
        -team_b["positive_xt"],
        width=4.2,
        label=teams[1],
    )

    ax.axhline(
        0,
        linewidth=1,
    )

    ax.axvline(
        45,
        linestyle="--",
        alpha=0.5,
    )

    ax.set_xlabel(
        "Match minute"
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

    print(
        f"xT momentum generated: {output_path}"
    )

    return output_path