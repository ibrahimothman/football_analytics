"""Serving-layer referential integrity checks."""

from __future__ import annotations

import pandas as pd


def validate_fact_match_references(
    fact_df: pd.DataFrame,
    dim_match_df: pd.DataFrame,
) -> None:
    """
    Validate match_id references between fact and dim_match.

    - Every dim_match match_id must appear in fact.
    - Every fact match_id must exist in dim_match.
    """

    errors: list[str] = []

    if "match_id" not in fact_df.columns:
        raise ValueError(
            "fact_df is missing required column: match_id"
        )

    if "match_id" not in dim_match_df.columns:
        raise ValueError(
            "dim_match_df is missing required column: match_id"
        )

    fact_ids = {
        int(match_id)
        for match_id in fact_df["match_id"].dropna()
    }
    dim_ids = {
        int(match_id)
        for match_id in dim_match_df["match_id"].dropna()
    }

    missing_from_fact = sorted(dim_ids - fact_ids)
    unknown_in_fact = sorted(fact_ids - dim_ids)

    if missing_from_fact:
        errors.append(
            f"{len(missing_from_fact)} dim_match "
            "match_id values missing from fact: "
            f"{missing_from_fact}"
        )

    if unknown_in_fact:
        errors.append(
            f"{len(unknown_in_fact)} fact match_id "
            "values not found in dim_match: "
            f"{unknown_in_fact}"
        )

    if errors:
        raise ValueError(
            "Fact ↔ dim_match reference validation failed: "
            + "; ".join(errors)
        )
