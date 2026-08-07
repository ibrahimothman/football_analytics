"""Build canonical Silver football events."""

from __future__ import annotations

import math
import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")

SOURCE_LENGTH = 120
SOURCE_WIDTH = 80

TARGET_LENGTH = 105
TARGET_WIDTH = 68

GOAL_X = 105
GOAL_Y = 34


def normalize_x(value: float | None) -> float | None:
    if pd.isna(value):
        return None

    return value / SOURCE_LENGTH * TARGET_LENGTH


def normalize_y(value: float | None) -> float | None:
    if pd.isna(value):
        return None

    return value / SOURCE_WIDTH * TARGET_WIDTH


def get_nested(
    data: dict[str, Any],
    *keys: str,
) -> Any:
    """Safely read a nested JSON value."""

    current: Any = data

    for key in keys:
        if not isinstance(current, dict):
            return None

        current = current.get(key)

    return current    

def get_end_location(
    event: dict[str, Any],
) -> list[float] | None:

    event_type = get_nested(
        event,
        "type",
        "name",
    )

    if event_type == "Pass":
        return get_nested(
            event,
            "pass",
            "end_location",
        )

    if event_type == "Carry":
        return get_nested(
            event,
            "carry",
            "end_location",
        )

    if event_type == "Shot":
        return get_nested(
            event,
            "shot",
            "end_location",
        )

    return None   

def is_completed_pass(
    event_type: str,
    outcome: str | None,
) -> bool:
    """Return whether a pass was completed."""

    return event_type == "Pass" and pd.isna(outcome)    


def distance_to_goal(
    x: float | None,
    y: float | None,
) -> float | None:
    """Calculate Euclidean distance to opponent goal centre."""

    if pd.isna(x) or pd.isna(y):
        return None

    return math.sqrt(
        (GOAL_X - x) ** 2
        + (GOAL_Y - y) ** 2
    )

def calculate_progression(
    start_x: float | None,
    start_y: float | None,
    end_x: float | None,
    end_y: float | None,
) -> tuple[float | None, float | None]:

    start_distance = distance_to_goal(
        start_x,
        start_y,
    )

    end_distance = distance_to_goal(
        end_x,
        end_y,
    )

    if (
        start_distance is None
        or end_distance is None
        or start_distance == 0
    ):
        return None, None

    distance_gained = (
        start_distance - end_distance
    )

    progress_ratio = (
        distance_gained / start_distance
    )

    return distance_gained, progress_ratio

def is_progressive_pass(
    is_completed: bool,
    progress_ratio: float | None,
) -> bool:

    if not is_completed:
        return False

    if progress_ratio is None:
        return False

    return progress_ratio >= 0.25    

def bronze_to_silver_row(
    row: pd.Series,
) -> dict:

    event = json.loads(
        row["event_payload_json"]
    )

    end_location = get_end_location(event)

    end_x_raw = None
    end_y_raw = None

    if end_location:
        if len(end_location) >= 2:
            end_x_raw = end_location[0]
            end_y_raw = end_location[1]

    event_type = row["event_type_name"]

    outcome = None
    shot_xg = None

    if event_type == "Pass":
        outcome = get_nested(
            event,
            "pass",
            "outcome",
            "name",
        )

    elif event_type == "Shot":
        outcome = get_nested(
            event,
            "shot",
            "outcome",
            "name",
        )

        shot_xg = get_nested(
            event,
            "shot",
            "statsbomb_xg",
        )

    start_x = normalize_x(row["location_x_raw"])
    start_y = normalize_y(row["location_y_raw"])
    end_x = normalize_x(end_x_raw)
    end_y = normalize_y(end_y_raw) 

    completed_pass = is_completed_pass(event_type, outcome)

    distance_gained, progress_ratio = calculate_progression(start_x, start_y, end_x, end_y)
    progressive_pass = is_progressive_pass(completed_pass, progress_ratio)


    

    return {
        "match_id": row["match_id"],
        "event_id": row["event_id"],
        "event_index": row["event_index"],

        "period": row["period"],
        "minute": row["minute"],
        "second": row["second"],

        "event_type": event_type,

        "team_id": row["team_id"],
        "team_name": row["team_name"],

        "player_id": row["player_id"],
        "player_name": row["player_name"],

        "possession_id": row["possession"],

        "start_x": start_x,
        "start_y": start_y,

        "end_x": end_x,
        "end_y": end_y,

        "outcome": outcome,
        "shot_xg": shot_xg,

        "is_pass": event_type == "Pass",
        "is_carry": event_type == "Carry",
        "is_shot": event_type == "Shot",

        "is_completed_pass": completed_pass,
        "is_progressive_pass": progressive_pass,
        "progress_ratio": progress_ratio,
        "progress_toward_goal_m": distance_gained,

        "source_version": row["source_version"],
        "file_hash": row["file_hash"],
    }  

def check_attacking_direction_using_shots(
    df: pd.DataFrame,
) -> None:

    shots = df[df["is_shot"]].copy()

    summary = (
        shots
        .groupby(
            ["team_name", "period"]
        )["start_x"]
        .median()
    )

    suspicious = summary[
        summary < 60
    ]

    if not suspicious.empty:
        print(
            "WARNING: possible coordinate "
            "orientation issue:"
        )
        print(suspicious)

def validate_silver(
    df: pd.DataFrame,
) -> None:

    if df.empty:
        raise ValueError(
            "Silver dataset contains no events."
        )

    if df["event_id"].isna().any():
        raise ValueError(
            "Silver contains missing event IDs."
        )

    if df["event_id"].duplicated().any():
        raise ValueError(
            "Silver contains duplicate event IDs."
        )

    if df["event_type"].isna().any():
        raise ValueError(
            "Silver contains events without event type."
        )

    invalid_x = df["start_x"].dropna().between(
        0,
        TARGET_LENGTH,
    ) == False

    if invalid_x.any():
        raise ValueError(
            "Silver contains invalid X coordinates."
        )

    invalid_y = df["start_y"].dropna().between(
        0,
        TARGET_WIDTH,
    ) == False

    if invalid_y.any():
        raise ValueError(
            "Silver contains invalid Y coordinates."
        )

    invalid_xg = (
        df["shot_xg"]
        .dropna()
        .between(0, 1)
        == False
    )

    if invalid_xg.any():
        raise ValueError(
            "Silver contains invalid shot xG."
        )       

    invalid_completed = (
    df["is_completed_pass"]
    & ~df["is_pass"]
    )

    if invalid_completed.any():
        raise ValueError(
            "Non-pass events marked as completed passes."
        )

    invalid_progressive = (
    df["is_progressive_pass"]
    & ~df["is_completed_pass"]
    )

    if invalid_progressive.any():
        raise ValueError(
            "Incomplete passes marked as progressive."
        )

    invalid_progress_ratio = (
    df.loc[
        df["is_progressive_pass"],
        "progress_ratio",
    ]
    < 0.25
    )

    if invalid_progress_ratio.any():
        raise ValueError(
            "Progressive pass below configured threshold."
        )            

def find_bronze_file(
    match_id: int,
) -> Path:

    folder = (
        BRONZE_DIR
        / f"match_id={match_id}"
    )

    files = list(
        folder.glob("events_*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            f"No Bronze data found for match {match_id}"
        )

    return max(
        files,
        key=lambda path: path.stat().st_mtime,
    )

def build_silver(
    match_id: int,
) -> Path:

    bronze_path = find_bronze_file(
        match_id
    )

    bronze_df = pd.read_parquet(
        bronze_path
    )

    rows = [
        bronze_to_silver_row(row)
        for _, row in bronze_df.iterrows()
    ]

    silver_df = pd.DataFrame(rows)

    validate_silver(silver_df)
    check_attacking_direction_using_shots(silver_df)

    match_directory = (
        SILVER_DIR
        / f"match_id={match_id}"
    )

    match_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_hash = (
        silver_df["file_hash"]
        .iloc[0][:12]
    )

    silver_path = (
        match_directory
        / f"events_{file_hash}.parquet"
    )

    silver_df.to_parquet(
        silver_path,
        index=False,
    )

    print()
    print("Silver build successful")
    print("-----------------------")
    print(f"Match:   {match_id}")
    print(f"Events:  {len(silver_df):,}")
    print(f"Output:  {silver_path}")

    print("\nEvent types:")
    print(
        silver_df["event_type"]
        .value_counts()
        .head(15)
        .to_string()
    )

    return silver_path   

def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    build_silver(args.match_id)


if __name__ == "__main__":
    main()