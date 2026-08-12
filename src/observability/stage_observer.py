from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
import json
from dataclasses import dataclass, field
from typing import Any
from contextlib import contextmanager
import uuid

from src.config import METADATA_DIR


STAGE_RUNS_PATH = (
    METADATA_DIR / "airflow_pipeline_stage_runs.jsonl"
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def append_jsonl(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        key: _jsonable(value)
        for key, value in data.items()
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


@dataclass
class PipelineRunStageRun:
    stage_run_id: str
    airflow_run_id: str
    dag_id: str
    task_id: str
    try_number: int
    match_id: int
    stage_name: str
    started_at: datetime | None
    status: str
    completed_at: datetime | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    error_type: str | None = None
    error_message: str | None = None
    duration_seconds: float | None = field(
        init=False,
        default=None,
    )        


@contextmanager
def observe_stage(
    airflow_run_id: str,
    dag_id: str,
    task_id: str,
    try_number: int,
    match_id: int,
    stage: str,
):
    started_at = utc_now()
    metrics = {
        "rows_in": None,
        "rows_out": None,
    }
    try:
        yield metrics
        completed_at = utc_now()
        append_jsonl(STAGE_RUNS_PATH, {
            "stage_run_id": str(uuid.uuid4()),
            "airflow_run_id": airflow_run_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "try_number": try_number,
            "match_id": match_id,
            "stage": stage,
            "status": "SUCCEEDED",
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_seconds": (completed_at - started_at).total_seconds(),
            "rows_in": metrics["rows_in"],
            "rows_out": metrics["rows_out"],
            "error_type": None,
            "error_message": None,
        })

    except Exception as e:
        completed_at = utc_now()
        append_jsonl(STAGE_RUNS_PATH, {
            "stage_run_id": str(uuid.uuid4()),
            "airflow_run_id": airflow_run_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "try_number": try_number,
            "match_id": match_id,
            "stage": stage,
            "status": "FAILED",
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_seconds": (completed_at - started_at).total_seconds(),
            "rows_in": metrics["rows_in"],
            "rows_out": metrics["rows_out"],
            "error_type": type(e).__name__,
            "error_message": str(e),
        })
        raise