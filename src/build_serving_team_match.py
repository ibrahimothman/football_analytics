"""Build serving fact_team_match from Gold team metrics."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from src.config.settings import (
    GOLD_DIR as SETTINGS_GOLD_DIR,
    SERVING_DIR as SETTINGS_SERVING_DIR,
)
from src.paths import latest_file


logger = logging.getLogger(__name__)

GOLD_DIR = SETTINGS_GOLD_DIR
SERVING_DIR = SETTINGS_SERVING_DIR

FACT_TEAM_MATCH_FILENAME = "fact_team_match.parquet"
GRAIN_KEYS = ("match_id", "team_id")


def discover_team_metric_files() -> list[Path]:
    """Return the latest team_metrics parquet per match partition."""

    if not GOLD_DIR.exists():
        raise FileNotFoundError(
            f"Gold directory not found at {GOLD_DIR}"
        )

    match_dirs = sorted(
        path
        for path in GOLD_DIR.iterdir()
        if path.is_dir() and path.name.startswith("match_id=")
    )

    files: list[Path] = []

    for match_dir in match_dirs:
        try:
            files.append(
                latest_file(
                    match_dir,
                    "team_metrics_*.parquet",
                )
            )
        except FileNotFoundError:
            logger.warning(
                "gold_team_metrics_missing",
                extra={"match_dir": str(match_dir)},
            )

    if not files:
        raise FileNotFoundError(
            f"No team_metrics parquet files found under {GOLD_DIR}"
        )

    return files


def validate_team_match_grain(
    df: pd.DataFrame,
) -> None:
    """Ensure serving grain is unique on match_id × team_id."""

    if df.empty:
        raise ValueError(
            "Serving fact_team_match is empty."
        )

    missing = [
        column
        for column in GRAIN_KEYS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "Serving fact_team_match missing grain columns: "
            + ", ".join(missing)
        )

    duplicated = df.duplicated(
        subset=list(GRAIN_KEYS),
        keep=False,
    )

    if duplicated.any():
        dupes = (
            df.loc[duplicated, list(GRAIN_KEYS)]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        raise ValueError(
            "Duplicate (match_id, team_id) rows in "
            f"fact_team_match: {dupes}"
        )


def build_serving_team_match() -> Path:
    """Consolidate Gold team metrics into fact_team_match.parquet."""

    metric_files = discover_team_metric_files()

    frames = [
        pd.read_parquet(path)
        for path in metric_files
    ]

    fact = pd.concat(frames, ignore_index=True)

    fact = fact.sort_values(
        list(GRAIN_KEYS),
        kind="mergesort",
    ).reset_index(drop=True)

    validate_team_match_grain(fact)

    SERVING_DIR.mkdir(parents=True, exist_ok=True)

    output_path = (
        SERVING_DIR / FACT_TEAM_MATCH_FILENAME
    )

    fact.to_parquet(output_path, index=False)

    logger.info(
        "serving_team_match_build_succeeded",
        extra={
            "matches": int(fact["match_id"].nunique()),
            "rows": len(fact),
            "source_files": len(metric_files),
            "output_path": str(output_path),
        },
    )

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Consolidate Gold team metrics into "
            "serving/fact_team_match.parquet"
        ),
    )
    parser.parse_args()
    build_serving_team_match()


if __name__ == "__main__":
    main()
