from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4
import json

from src.config.settings import (
    PIPELINE_RUNS_PATH,
    PIPELINE_STAGE_RUNS_PATH,
)


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


class PipelineRunStage(str, Enum):
    INGEST = "ingest"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD_TEAM = "gold_team"
    GOLD_INTERVAL = "gold_interval"
    REPORTS = "reports"


class PipelineRunStatus(str, Enum):
    RUNNING = "running"
    NOT_RUN = "not_run"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


PIPELINE_STAGES: tuple[PipelineRunStage, ...] = (
    PipelineRunStage.INGEST,
    PipelineRunStage.BRONZE,
    PipelineRunStage.SILVER,
    PipelineRunStage.GOLD_TEAM,
    PipelineRunStage.GOLD_INTERVAL,
    PipelineRunStage.REPORTS,
)


def remaining_stages(
    failed_stage: PipelineRunStage,
) -> tuple[PipelineRunStage, ...]:
    """Return stages that were skipped after a failure."""

    index = PIPELINE_STAGES.index(failed_stage)
    return PIPELINE_STAGES[index + 1 :]



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PipelineRun:
    run_id: str
    match_id: int
    started_at: datetime
    status: PipelineRunStatus
    completed_at: datetime | None = None
    failed_stage: PipelineRunStage | None = None
    error_type: str | None = None
    error_message: str | None = None
    duration_seconds: float | None = field(
        init=False,
        default=None,
    )

    def __post_init__(self) -> None:
        if self.completed_at is not None:
            self.duration_seconds = (
                self.completed_at
                - self.started_at
            ).total_seconds()
        else:
            self.duration_seconds = None


@dataclass
class PipelineRunStageRun:
    stage_run_id: str
    run_id: str
    match_id: int
    stage_name: PipelineRunStage
    started_at: datetime | None
    status: PipelineRunStatus
    completed_at: datetime | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    error_type: str | None = None
    error_message: str | None = None
    duration_seconds: float | None = field(
        init=False,
        default=None,
    )

    def __post_init__(self) -> None:
        if self.completed_at is not None and self.started_at is not None:
            self.duration_seconds = (
                self.completed_at
                - self.started_at
            ).total_seconds()
        else:
            self.duration_seconds = None


def new_run_id() -> str:
    return str(uuid4())

def new_stage_run_id() -> str:
    return str(uuid4())

def write_pipeline_run(
    run_id: str,
    match_id: int,
    started_at: datetime,
    completed_at: datetime,
    status: PipelineRunStatus,
    failed_stage: PipelineRunStage | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> PipelineRun:
    """Record one completed pipeline execution."""


    record = PipelineRun(
        run_id=run_id,
        match_id=match_id,
        started_at=started_at,
        completed_at=completed_at,
        status=status,
        failed_stage=failed_stage,
        error_type=error_type,
        error_message=error_message,
    )

    append_jsonl(
        PIPELINE_RUNS_PATH,
        asdict(record),
    )

    return record


def write_stage_run(
    stage_run_id: str,
    run_id: str,
    match_id: int,
    stage_name: PipelineRunStage,
    started_at: datetime | None,
    completed_at: datetime | None,
    status: PipelineRunStatus,
    rows_in: int | None = None,
    rows_out: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> PipelineRunStageRun:

    record = PipelineRunStageRun(
        stage_run_id=stage_run_id,
        run_id=run_id,
        match_id=match_id,
        stage_name=stage_name,
        started_at=started_at,
        completed_at=completed_at,
        status=status,
        rows_in=rows_in,
        rows_out=rows_out,
        error_type=error_type,
        error_message=error_message,
    )

    append_jsonl(
        PIPELINE_STAGE_RUNS_PATH,
        asdict(record),
    )

    return record