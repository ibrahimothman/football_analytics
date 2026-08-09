from pathlib import Path
from pytest import MonkeyPatch
import hashlib
import json

import pandas as pd

import src.build_bronze as bronze
import src.build_silver as silver
import src.build_gold as gold
import src.build_gold_intervals as gold_intervals




MATCH_ID = 999000
def create_mock_xt_grid():
    """create a mock xt 12x8 grid"""
    return [
        [
            column * 0.01 for column in range(12)
        ] for row in range(8)
    ]

def test_raw_to_gold_pipeline(tmp_path: Path, monkeypatch: MonkeyPatch):

    metadata_dir = tmp_path / "metadata"
    manifest_path = metadata_dir / "ingestion_manifest.jsonl"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    fixture_path = Path(__file__).parent / "sample_events.json"
    fixture_content = fixture_path.read_bytes()
    fixture_hash = hashlib.sha256(fixture_content).hexdigest()

    manifest_record = {
        "ingestion_id": "test-ingestion-001",
        "match_id": MATCH_ID,
        "provider": "test_provider",
        "source_version": 1,
        "source_url": "fixtures/sample_events.json",
        "file_hash": fixture_hash,
        "raw_path": str(fixture_path),
        "ingested_at": (
            "2026-08-09T00:00:00+00:00"
        ),
    }


    metadata_dir.mkdir(parents=True, exist_ok=True)

    manifest_path.write_text(json.dumps(manifest_record) + "\n", encoding="utf-8")

    #
    # Redirect pipeline paths to temporary
    # test directories.
    #
    monkeypatch.setattr(bronze, "MANIFEST_PATH", manifest_path)
    monkeypatch.setattr(bronze, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(silver, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(silver, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(silver, "XT_MODEL_VERSION", "test_xt_model_v1")
    monkeypatch.setattr(silver, "load_xt_grid", create_mock_xt_grid)
    monkeypatch.setattr(gold, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(gold, "GOLD_DIR", gold_dir)
    monkeypatch.setattr(gold_intervals, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(gold_intervals, "GOLD_DIR", gold_dir)

    bronze_path = bronze.build_bronze(MATCH_ID)
    silver_path = silver.build_silver(MATCH_ID)
    gold_path = gold.build_gold(MATCH_ID)
    gold_intervals_path = gold_intervals.build_gold_intervals(MATCH_ID)

    # read output files
    bronze_df = pd.read_parquet(bronze_path)
    silver_df = pd.read_parquet(silver_path)
    gold_df = pd.read_parquet(gold_path)
    gold_intervals_df = pd.read_parquet(gold_intervals_path)

    # assert pipeline grain.
    assert len(bronze_df) == 18
    assert len(silver_df) == 18
    assert len(gold_df) == 2 # 1 per team
    assert len(gold_intervals_df) == 40 # 20 intervals per team

    # assert silver semantics
    assert silver_df["event_id"].nunique() == 18
    progressive_passes = silver_df[
        silver_df["is_progressive_pass"]
    ]

    assert len(progressive_passes) == 3

    # assert Arsenal gold metrics
    arsenal = (gold_df[gold_df["team_name"] == "Arsenal"]).iloc[0]
    assert arsenal["goals"] == 2
    assert arsenal["shots"] == 3
    assert arsenal["passes_attempted"] == 4
    assert arsenal["passes_completed"] == 4
    assert arsenal["progressive_passes"] == 2
    assert arsenal["pass_completion_pct"] == 100.0
    assert arsenal["xg"] == 1.3735

    # assert Middlesbrough United gold metrics
    manchester_united = (gold_df[gold_df["team_name"] == "Middlesbrough"]).iloc[0]
    assert manchester_united["goals"] == 0
    assert manchester_united["shots"] == 0
    assert manchester_united["passes_attempted"] == 2
    assert manchester_united["passes_completed"] == 1
    assert manchester_united["progressive_passes"] == 1
    assert manchester_united["pass_completion_pct"] == 50.0
    assert manchester_united["xg"] == 0.0

    # assert period-aware intervals
    expected_labels = {
        "0-5",
        "5-10",
        "10-15",
        "15-20",
        "20-25",
        "25-30",
        "30-35",
        "35-40",
        "40-45",
        "45+",
        "45-50",
        "50-55",
        "55-60",
        "60-65",
        "65-70",
        "70-75",
        "75-80",
        "80-85",
        "85-90",
        "90+",
    }
    assert set(gold_intervals_df["interval_label"].unique()) == expected_labels

    first_half_stoppage = gold_intervals_df[gold_intervals_df["interval_label"] == "45+"]
    second_half_start = gold_intervals_df[gold_intervals_df["interval_label"] == "45-50"]
    assert first_half_stoppage["period"].eq(1).all()
    assert second_half_start["period"].eq(2).all()
    