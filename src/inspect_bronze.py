"""Inspect a Bronze event Parquet file."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path",
        type=Path,
        required=True,
    )

    args = parser.parse_args()

    df = pd.read_parquet(args.path)

    print("\nSHAPE")
    print(df.shape)

    print("\nCOLUMNS")
    print(df.columns.tolist())

    print("\nEVENT TYPES")
    print(
        df["event_type_name"]
        .value_counts()
        .to_string()
    )

    print("\nTEAMS")
    print(
        df["team_name"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nSAMPLE EVENTS")
    print(
        df[
            [
                "event_index",
                "minute",
                "second",
                "team_name",
                "player_name",
                "event_type_name",
                "location_x_raw",
                "location_y_raw",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()