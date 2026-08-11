import pandas as pd
import pytest

from src.quality.serving import validate_fact_match_references


def test_validate_fact_match_references_passes_when_aligned():
    fact_df = pd.DataFrame(
        {
            "match_id": [1, 1, 2, 2],
            "team_id": [10, 20, 10, 30],
        }
    )
    dim_match_df = pd.DataFrame(
        {
            "match_id": [1, 2],
            "home_team_name": ["A", "B"],
        }
    )

    validate_fact_match_references(fact_df, dim_match_df)


def test_validate_fact_match_references_rejects_missing_fact_matches():
    fact_df = pd.DataFrame({"match_id": [1, 1]})
    dim_match_df = pd.DataFrame({"match_id": [1, 2]})

    with pytest.raises(
        ValueError,
        match="missing from fact",
    ):
        validate_fact_match_references(fact_df, dim_match_df)


def test_validate_fact_match_references_rejects_unknown_fact_matches():
    fact_df = pd.DataFrame({"match_id": [1, 99]})
    dim_match_df = pd.DataFrame({"match_id": [1]})

    with pytest.raises(
        ValueError,
        match="not found in dim_match",
    ):
        validate_fact_match_references(fact_df, dim_match_df)
