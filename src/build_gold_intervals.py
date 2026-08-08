"""Build 5-minute Gold match interval metrics."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")

INTERVAL_MINUTES = 5
METRIC_VERSION = "1.0"


def find_silver_file(
    match_id: int,
) -> Path:

    folder = (
        SILVER_DIR
        / f"match_id={match_id}"
    )

    files = list(
        folder.glob("events_*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            f"No Silver data for match {match_id}."
        )

    return max(
        files,
        key=lambda path: path.stat().st_mtime,
    )

def add_interval(
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Assign every event to a fixed 5-minute interval."""

    events = events.copy()

    events["interval_start"] = (
        events["minute"]
        // INTERVAL_MINUTES
        * INTERVAL_MINUTES
    ).astype(int)

    events["interval_end"] = (
        events["interval_start"]
        + INTERVAL_MINUTES
    )

    return events    

def calculate_interval_metrics(
    events: pd.DataFrame,
) -> pd.DataFrame:

    events = add_interval(events)

    rows = []

    grouped = events.groupby(
        [
            "match_id",
            "team_id",
            "team_name",
            "interval_start",
            "interval_end",
        ],
        dropna=False,
    )

    for keys, group in grouped:

        (
            match_id,
            team_id,
            team_name,
            interval_start,
            interval_end,
        ) = keys

        moves = group[
            group["is_successful_move"]
        ]

        positive_xt = (
            moves.loc[
                moves["xt_added"] > 0,
                "xt_added",
            ]
            .fillna(0)
            .sum()
        )

        negative_xt = (
            moves.loc[
                moves["xt_added"] < 0,
                "xt_added",
            ]
            .fillna(0)
            .sum()
        )

        net_xt = (
            moves["xt_added"]
            .fillna(0)
            .sum()
        )

        shots = group[
            group["is_shot"]
        ]

        interval_xg = (
            shots["shot_xg"]
            .fillna(0)
            .sum()
        )

        rows.append(
            {
                "match_id": match_id,
                "team_id": team_id,
                "team_name": team_name,

                "interval_start": interval_start,
                "interval_end": interval_end,

                "positive_xt": float(
                    positive_xt
                ),

                "negative_xt": float(
                    negative_xt
                ),

                "net_xt": float(
                    net_xt
                ),

                "successful_moves": int(
                    len(moves)
                ),

                "shots": int(
                    len(shots)
                ),

                "xg": float(
                    interval_xg
                ),
            }
        )

    return pd.DataFrame(rows)    


def densify_intervals(
    metrics: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Ensure every team has every match interval."""

    teams = (
        events[
            ["team_id", "team_name"]
        ]
        .dropna(subset=["team_id"])
        .drop_duplicates()
    )

    max_minute = int(
        events["minute"].max()
    )

    max_interval_start = (
        max_minute
        // INTERVAL_MINUTES
        * INTERVAL_MINUTES
    )

    intervals = range(
        0,
        max_interval_start + INTERVAL_MINUTES,
        INTERVAL_MINUTES,
    )

    match_id = int(
        events["match_id"].iloc[0]
    )

    complete_rows = []

    for _, team in teams.iterrows():
        for interval_start in intervals:

            complete_rows.append(
                {
                    "match_id": match_id,
                    "team_id": team["team_id"],
                    "team_name": team["team_name"],
                    "interval_start": interval_start,
                    "interval_end": (
                        interval_start
                        + INTERVAL_MINUTES
                    ),
                }
            )

    complete = pd.DataFrame(
        complete_rows
    )

    result = complete.merge(
        metrics,
        on=[
            "match_id",
            "team_id",
            "team_name",
            "interval_start",
            "interval_end",
        ],
        how="left",
    )

    metric_columns = [
        "positive_xt",
        "negative_xt",
        "net_xt",
        "successful_moves",
        "shots",
        "xg",
    ]

    result[metric_columns] = (
        result[metric_columns]
        .fillna(0)
    )

    return result    


def validate_intervals(
    df: pd.DataFrame,
) -> None:

    if df.empty:
        raise ValueError(
            "Gold interval dataset is empty."
        )

    duplicate_keys = df.duplicated(
        subset=[
            "match_id",
            "team_id",
            "interval_start",
        ]
    )

    if duplicate_keys.any():
        raise ValueError(
            "Duplicate team/interval grain detected."
        )

    if (
        df["interval_start"]
        % INTERVAL_MINUTES
        != 0
    ).any():
        raise ValueError(
            "Invalid interval boundary."
        )

    if (df["positive_xt"] < 0).any():
        raise ValueError(
            "Positive xT cannot be negative."
        )

    if (df["negative_xt"] > 0).any():
        raise ValueError(
            "Negative xT cannot be positive."
        )

    expected_net = (
        df["positive_xt"]
        + df["negative_xt"]
    )

    difference = (
        expected_net
        - df["net_xt"]
    ).abs()

    if (difference > 1e-10).any():
        raise ValueError(
            "Net xT does not equal positive + negative xT."
        )    


def build_gold_intervals(
    match_id: int,
) -> Path:

    silver_path = find_silver_file(
        match_id
    )

    events = pd.read_parquet(
        silver_path
    )

    metrics = calculate_interval_metrics(
        events
    )

    metrics = densify_intervals(
        metrics,
        events,
    )

    metrics["source_version"] = (
        events["source_version"].iloc[0]
    )

    metrics["file_hash"] = (
        events["file_hash"].iloc[0]
    )

    metrics["xt_model_version"] = (
        events["xt_model_version"].iloc[0]
    )

    metrics["metric_version"] = (
        METRIC_VERSION
    )

    validate_intervals(metrics)

    match_directory = (
        GOLD_DIR
        / f"match_id={match_id}"
    )

    match_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    short_hash = (
        events["file_hash"]
        .iloc[0][:12]
    )

    output_path = (
        match_directory
        / f"team_intervals_{short_hash}.parquet"
    )

    metrics.to_parquet(
        output_path,
        index=False,
    )

    print()
    print("Gold interval build successful")
    print("------------------------------")

    print(
        metrics[
            [
                "team_name",
                "interval_start",
                "interval_end",
                "positive_xt",
                "negative_xt",
                "net_xt",
                "shots",
                "xg",
            ]
        ].to_string(index=False)
    )

    print()
    print(f"Output: {output_path}")

    return output_path       


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    build_gold_intervals(
        args.match_id
    )


if __name__ == "__main__":
    main()     