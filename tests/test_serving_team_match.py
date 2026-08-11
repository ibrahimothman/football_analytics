import os
from pathlib import Path

import pandas as pd
import pytest

import src.build_serving_team_match as serving


def _write_team_metrics(
    gold_dir: Path,
    match_id: int,
    rows: list[dict],
    *,
    filename: str = "team_metrics_abc123.parquet",
) -> Path:
    match_dir = gold_dir / f"match_id={match_id}"
    match_dir.mkdir(parents=True, exist_ok=True)
    path = match_dir / filename
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def test_build_serving_team_match_consolidates_gold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    gold_dir = tmp_path / "gold"
    serving_dir = tmp_path / "serving"

    monkeypatch.setattr(serving, "GOLD_DIR", gold_dir)
    monkeypatch.setattr(serving, "SERVING_DIR", serving_dir)

    _write_team_metrics(
        gold_dir,
        1,
        [
            {
                "match_id": 1,
                "team_id": 10,
                "team_name": "A",
                "goals": 2,
            },
            {
                "match_id": 1,
                "team_id": 20,
                "team_name": "B",
                "goals": 1,
            },
        ],
    )
    _write_team_metrics(
        gold_dir,
        2,
        [
            {
                "match_id": 2,
                "team_id": 10,
                "team_name": "A",
                "goals": 0,
            },
            {
                "match_id": 2,
                "team_id": 30,
                "team_name": "C",
                "goals": 3,
            },
        ],
    )

    output_path = serving.build_serving_team_match()

    fact = pd.read_parquet(output_path)

    assert output_path.name == "fact_team_match.parquet"
    assert len(fact) == 4
    assert fact["match_id"].nunique() == 2
    assert (
        fact.duplicated(
            subset=["match_id", "team_id"]
        ).sum()
        == 0
    )


def test_build_serving_team_match_uses_latest_per_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    gold_dir = tmp_path / "gold"
    serving_dir = tmp_path / "serving"

    monkeypatch.setattr(serving, "GOLD_DIR", gold_dir)
    monkeypatch.setattr(serving, "SERVING_DIR", serving_dir)

    old_path = _write_team_metrics(
        gold_dir,
        1,
        [
            {
                "match_id": 1,
                "team_id": 10,
                "team_name": "A",
                "goals": 1,
            },
            {
                "match_id": 1,
                "team_id": 20,
                "team_name": "B",
                "goals": 0,
            },
        ],
        filename="team_metrics_old.parquet",
    )
    new_path = _write_team_metrics(
        gold_dir,
        1,
        [
            {
                "match_id": 1,
                "team_id": 10,
                "team_name": "A",
                "goals": 4,
            },
            {
                "match_id": 1,
                "team_id": 20,
                "team_name": "B",
                "goals": 2,
            },
        ],
        filename="team_metrics_new.parquet",
    )

    older = old_path.stat().st_mtime - 10
    newer = old_path.stat().st_mtime + 10
    os.utime(old_path, (older, older))
    os.utime(new_path, (newer, newer))

    output_path = serving.build_serving_team_match()
    fact = pd.read_parquet(output_path)

    assert len(fact) == 2
    assert int(
        fact.loc[
            fact["team_id"] == 10,
            "goals",
        ].iloc[0]
    ) == 4


def test_validate_team_match_grain_rejects_duplicates():
    df = pd.DataFrame(
        [
            {"match_id": 1, "team_id": 10},
            {"match_id": 1, "team_id": 10},
        ]
    )

    with pytest.raises(ValueError, match="Duplicate"):
        serving.validate_team_match_grain(df)
