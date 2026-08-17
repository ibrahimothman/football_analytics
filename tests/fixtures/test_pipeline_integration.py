import hashlib
from pathlib import Path
import json

from src.transforms.bronze import events_to_bronze
from src.transforms.silver import bronze_to_silver


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

    # assert pipeline grain.
    assert len(bronze_df) == 18
    assert len(silver_df) == 18

    # assert silver semantics
    assert silver_df["event_id"].nunique() == 18
    progressive_passes = silver_df[
        silver_df["is_progressive_pass"]
    ]
    assert len(progressive_passes) == 3