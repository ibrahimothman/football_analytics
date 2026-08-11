from pathlib import Path

import pandas as pd
import pytest

import src.build_dim_match as dim_match


def _sample_matches() -> list[dict]:
    return [
        {
            "match_id": 303731,
            "match_date": "2020-02-22",
            "kick_off": "16:00:00.000",
            "competition": {
                "competition_id": 11,
                "country_name": "Spain",
                "competition_name": "La Liga",
            },
            "season": {
                "season_id": 42,
                "season_name": "2019/2020",
            },
            "home_team": {
                "home_team_id": 217,
                "home_team_name": "Barcelona",
                "home_team_gender": "male",
                "home_team_group": None,
            },
            "away_team": {
                "away_team_id": 322,
                "away_team_name": "Eibar",
                "away_team_gender": "male",
                "away_team_group": None,
            },
            "home_score": 5,
            "away_score": 0,
        },
        {
            "match_id": 303700,
            "match_date": "2020-02-15",
            "competition": {
                "competition_id": 11,
                "competition_name": "La Liga",
            },
            "season": {
                "season_id": 42,
                "season_name": "2019/2020",
            },
            "home_team": {
                "home_team_id": 322,
                "home_team_name": "Eibar",
            },
            "away_team": {
                "away_team_id": 217,
                "away_team_name": "Barcelona",
            },
        },
    ]


def test_match_to_dim_row_reads_nested_fields():
    row = dim_match.match_to_dim_row(
        _sample_matches()[0]
    )

    assert row == {
        "match_id": 303731,
        "match_date": "2020-02-22",
        "competition_id": 11,
        "competition_name": "La Liga",
        "season_id": 42,
        "season_name": "2019/2020",
        "home_team_id": 217,
        "home_team_name": "Barcelona",
        "away_team_id": 322,
        "away_team_name": "Eibar",
    }


def test_build_dim_match_writes_unique_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        dim_match,
        "SERVING_DIR",
        tmp_path / "serving",
    )
    monkeypatch.setattr(
        dim_match,
        "fetch_matches",
        lambda competition_id, season_id: _sample_matches(),
    )

    output_path = dim_match.build_dim_match(
        competition_id=11,
        season_id=42,
    )
    fact = pd.read_parquet(output_path)

    assert output_path.name == "dim_match.parquet"
    assert len(fact) == 2
    assert fact["match_id"].is_unique
    assert list(fact["match_id"]) == [303700, 303731]
    assert list(fact.columns) == dim_match.DIM_MATCH_COLUMNS


def test_build_dim_match_merges_existing_and_keeps_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    serving_dir = tmp_path / "serving"
    serving_dir.mkdir(parents=True)
    output_path = serving_dir / dim_match.DIM_MATCH_FILENAME

    existing = pd.DataFrame(
        [
            {
                "match_id": 303731,
                "match_date": "2020-02-22",
                "competition_id": 11,
                "competition_name": "La Liga",
                "season_id": 42,
                "season_name": "2019/2020",
                "home_team_id": 217,
                "home_team_name": "Old Barcelona Name",
                "away_team_id": 322,
                "away_team_name": "Eibar",
            },
            {
                "match_id": 999001,
                "match_date": "2003-08-16",
                "competition_id": 2,
                "competition_name": "Premier League",
                "season_id": 44,
                "season_name": "2003/2004",
                "home_team_id": 1,
                "home_team_name": "Arsenal",
                "away_team_id": 29,
                "away_team_name": "Everton",
            },
        ],
        columns=dim_match.DIM_MATCH_COLUMNS,
    )
    existing.to_parquet(output_path, index=False)

    monkeypatch.setattr(dim_match, "SERVING_DIR", serving_dir)
    monkeypatch.setattr(
        dim_match,
        "fetch_matches",
        lambda competition_id, season_id: _sample_matches(),
    )

    dim_match.build_dim_match(
        competition_id=11,
        season_id=42,
    )
    fact = pd.read_parquet(output_path)

    assert len(fact) == 3
    assert set(fact["match_id"]) == {303700, 303731, 999001}

    updated = fact.loc[
        fact["match_id"] == 303731
    ].iloc[0]
    assert updated["home_team_name"] == "Barcelona"

    kept = fact.loc[
        fact["match_id"] == 999001
    ].iloc[0]
    assert kept["home_team_name"] == "Arsenal"



