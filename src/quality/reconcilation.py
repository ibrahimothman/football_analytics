import math
import pandas as pd


def reconcile_silver_to_gold_team(
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
) -> None:

    errors = []

    for _, gold_row in gold_df.iterrows():
        team_id = gold_row["team_id"]

        team_events = silver_df[
            silver_df["team_id"] == team_id
        ]

        expected_shots = int(
            team_events["is_shot"].sum()
        )

        expected_completed_passes = int(
            team_events[
                "is_completed_pass"
            ].sum()
        )

        expected_xg = float(
            team_events["shot_xg"]
            .fillna(0)
            .sum()
        )

        if (
            expected_shots
            != gold_row["shots"]
        ):
            errors.append(
                f"team_id={team_id}: "
                f"shots expected={expected_shots}, "
                f"actual={gold_row['shots']}"
            )

        if (
            expected_completed_passes
            != gold_row["passes_completed"]
        ):
            errors.append(
                f"team_id={team_id}: "
                "completed passes mismatch"
            )

        if not math.isclose(
            expected_xg,
            float(gold_row["xg"]),
            abs_tol=1e-6,
        ):
            errors.append(
                f"team_id={team_id}: "
                f"xG expected={expected_xg}, "
                f"actual={gold_row['xg']}"
            )

    if errors:
        raise ValueError(
            "Silver → Gold Team "
            "reconciliation failed: "
            + "; ".join(errors)
        )

def reconcile_bronze_to_silver(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
) -> None:
    errors = []

    bronze_count = len(bronze_df)
    silver_count = len(silver_df)

    if bronze_count != silver_count:
        errors.append(
            f"row count mismatch: "
            f"bronze={bronze_count}, "
            f"silver={silver_count}"
        )

    bronze_ids = set(
        bronze_df["event_id"]
    )

    silver_ids = set(
        silver_df["event_id"]
    )

    missing_ids = (
        bronze_ids - silver_ids
    )

    unexpected_ids = (
        silver_ids - bronze_ids
    )

    if missing_ids:
        errors.append(
            f"{len(missing_ids)} "
            "Bronze events missing from Silver"
        )

    if unexpected_ids:
        errors.append(
            f"{len(unexpected_ids)} "
            "unexpected Silver event IDs"
        )

    if errors:
        raise ValueError(
            "Bronze → Silver reconciliation failed: "
            + "; ".join(errors)
        )        