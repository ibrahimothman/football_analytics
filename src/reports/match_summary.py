"""Generate a match summary from Gold team metrics."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REPORTS_DIR = Path("reports")


def generate_match_summary(
    gold_path: Path,
) -> Path:
    """Generate a simple match summary image."""

    df = pd.read_parquet(gold_path)

    if len(df) != 2:
        raise ValueError(
            "Expected exactly two teams in Gold data."
        )

    match_id = int(df["match_id"].iloc[0])

    team_a = df.iloc[0]
    team_b = df.iloc[1]

    metrics = [
        ("Goals", "goals", "{:.0f}"),
        ("Shots", "shots", "{:.0f}"),
        ("xG", "xg", "{:.2f}"),
        (
            "Pass completion",
            "pass_completion_pct",
            "{:.1f}%",
        ),
        (
            "Progressive passes",
            "progressive_passes",
            "{:.0f}",
        ),
        ("Carries", "carries", "{:.0f}"),
    ]

    fig, ax = plt.subplots(
        figsize=(10, 7)
    )

    ax.axis("off")

    ax.text(
        0.5,
        0.93,
        f"{team_a['team_name']} vs {team_b['team_name']}",
        ha="center",
        va="center",
        fontsize=20,
        fontweight="bold",
    )

    ax.text(
        0.25,
        0.84,
        team_a["team_name"],
        ha="center",
        fontsize=14,
        fontweight="bold",
    )

    ax.text(
        0.75,
        0.84,
        team_b["team_name"],
        ha="center",
        fontsize=14,
        fontweight="bold",
    )

    y = 0.73

    for label, column, fmt in metrics:

        value_a = team_a[column]
        value_b = team_b[column]

        ax.text(
            0.25,
            y,
            fmt.format(value_a),
            ha="center",
            fontsize=14,
        )

        ax.text(
            0.5,
            y,
            label,
            ha="center",
            fontsize=12,
        )

        ax.text(
            0.75,
            y,
            fmt.format(value_b),
            ha="center",
            fontsize=14,
        )

        y -= 0.10

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
        / "summary.png"
    )

    fig.savefig(
        output_path,
        dpi=150,
        bbox_inches="tight",
    )

    plt.close(fig)

    print(
        f"Match summary generated: {output_path}"
    )

    return output_path