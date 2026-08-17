import pandas as pd


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