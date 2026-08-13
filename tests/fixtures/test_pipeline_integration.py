import hashlib
from pathlib import Path
import json

from src.transforms.bronze import events_to_bronze
from src.transforms.silver import bronze_to_silver
from src.transforms.gold_team import calculate_team_metrics
from src.transforms.gold_intervals import build_interval_metrics

MATCH_ID = 3749358


def create_mock_xt_grid():
    """create a mock xt 12x8 grid"""
    return [
        [
            column * 0.01 for column in range(12)
        ] for row in range(8)
    ]


def test_pipeline_integration():
    fixture_path = Path(__file__).parent / "sample_events.json"
    fixture_content = fixture_path.read_bytes()
    events = json.loads(fixture_content)
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

    bronze_df = events_to_bronze(events, manifest_record)

    silver_df = bronze_to_silver(bronze_df, create_mock_xt_grid())

    gold_team_metrics = calculate_team_metrics(silver_df)
    gold_intervals_metrics = build_interval_metrics(silver_df)

    # assert pipeline grain.
    assert len(bronze_df) == 18
    assert len(silver_df) == 18
    assert len(gold_team_metrics) == 2 # 1 per team
    assert len(gold_intervals_metrics) == 40 # 20 intervals per team

    # assert silver semantics
    assert silver_df["event_id"].nunique() == 18
    progressive_passes = silver_df[
        silver_df["is_progressive_pass"]
    ]
    assert len(progressive_passes) == 3

    # assert Arsenal gold metrics
    arsenal = (gold_team_metrics[gold_team_metrics["team_name"] == "Arsenal"]).iloc[0]
    assert arsenal["goals"] == 2
    assert arsenal["shots"] == 3
    assert arsenal["passes_attempted"] == 4
    assert arsenal["passes_completed"] == 4
    assert arsenal["progressive_passes"] == 2
    assert arsenal["pass_completion_pct"] == 100.0
    assert arsenal["xg"] == 1.3735

    # assert Middlesbrough United gold metrics
    manchester_united = (gold_team_metrics[gold_team_metrics["team_name"] == "Middlesbrough"]).iloc[0]
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
    assert set(gold_intervals_metrics["interval_label"].unique()) == expected_labels

    first_half_stoppage = gold_intervals_metrics[gold_intervals_metrics["interval_label"] == "45+"]
    second_half_start = gold_intervals_metrics[gold_intervals_metrics["interval_label"] == "45-50"]
    assert first_half_stoppage["period"].eq(1).all()
    assert second_half_start["period"].eq(2).all()