"""Generate match reports from Silver and Gold data."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.reports.match_summary import (
    generate_match_summary,
)
from src.reports.shot_map import (
    generate_shot_map,
)
from src.reports.xg_timeline import (
    generate_xg_timeline,
)


SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")


def latest_file(
    folder: Path,
    pattern: str,
) -> Path:

    files = list(
        folder.glob(pattern)
    )

    if not files:
        raise FileNotFoundError(
            f"No files found in {folder}"
        )

    return max(
        files,
        key=lambda path: path.stat().st_mtime,
    )


def generate_reports(
    match_id: int,
) -> None:

    silver_path = latest_file(
        SILVER_DIR / f"match_id={match_id}",
        "events_*.parquet",
    )

    gold_path = latest_file(
        GOLD_DIR / f"match_id={match_id}",
        "team_metrics_*.parquet",
    )

    generate_match_summary(
        gold_path
    )

    generate_shot_map(
        silver_path
    )

    generate_xg_timeline(
        silver_path
    )


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    generate_reports(
        args.match_id
    )


if __name__ == "__main__":
    main()