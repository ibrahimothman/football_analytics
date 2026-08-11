"""Central runtime settings loaded from environment / .env."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


DATA_ROOT = Path(
    os.getenv("FOOTBALL_DATA_ROOT", "data")
)

REPORTS_ROOT = Path(
    os.getenv("FOOTBALL_REPORTS_ROOT", "reports")
)

MODELS_ROOT = Path(
    os.getenv("FOOTBALL_MODELS_ROOT", "models")
)

RAW_DIR = DATA_ROOT / "raw"
BRONZE_DIR = DATA_ROOT / "bronze"
SILVER_DIR = DATA_ROOT / "silver"
GOLD_DIR = DATA_ROOT / "gold"
SERVING_DIR = DATA_ROOT / "serving"
METADATA_DIR = DATA_ROOT / "metadata"
LOG_DIR = DATA_ROOT / "logs"

INGESTION_MANIFEST_PATH = (
    METADATA_DIR / "ingestion_manifest.jsonl"
)
PIPELINE_RUNS_PATH = (
    METADATA_DIR / "pipeline_runs.jsonl"
)
PIPELINE_STAGE_RUNS_PATH = (
    METADATA_DIR / "pipeline_stage_runs.jsonl"
)
AIRFLOW_EVENTS_PATH = (
    METADATA_DIR / "airflow_events.jsonl"
)
PIPELINE_LOG_PATH = LOG_DIR / "pipeline.jsonl"


STATSBOMB_EVENTS_BASE_URL = os.getenv(
    "STATSBOMB_EVENTS_BASE_URL",
    (
        "https://raw.githubusercontent.com/"
        "statsbomb/open-data/master/data/events"
    ),
)

STATSBOMB_MATCHES_BASE_URL = os.getenv(
    "STATSBOMB_MATCHES_BASE_URL",
    (
        "https://raw.githubusercontent.com/"
        "statsbomb/open-data/master/data/matches"
    ),
)

STATSBOMB_EVENTS_URL = (
    f"{STATSBOMB_EVENTS_BASE_URL.rstrip('/')}"
    "/{match_id}.json"
)

STATSBOMB_MATCHES_URL = (
    f"{STATSBOMB_MATCHES_BASE_URL.rstrip('/')}"
    "/{competition_id}/{season_id}.json"
)

HTTP_TIMEOUT_SECONDS = int(
    os.getenv("HTTP_TIMEOUT_SECONDS", "30")
)

PROVIDER = os.getenv(
    "FOOTBALL_PROVIDER",
    "statsbomb_open_data",
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
