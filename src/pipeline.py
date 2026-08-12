"""Run the match pipeline with pinned stage artifacts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.build_bronze import build_bronze
from src.build_gold import build_gold
from src.build_gold_intervals import build_gold_intervals
from src.build_silver import build_silver
from src.generate_reports import generate_reports
from src.ingest_match import ingest_match
from src.metadata.run_tracker import (
    PipelineRunStage,
    PipelineRunStageRun,
    PipelineRunStatus,
    new_run_id,
    new_stage_run_id,
    remaining_stages,
    utc_now,
    write_pipeline_run,
    write_stage_run,
)
from src.observability.logging_config import (
    clear_stage_context,
    configure_logging,
    set_run_context,
    set_stage_context,
)


logger = logging.getLogger(__name__)


def count_output_rows(result: Any) -> int | None:
    """Best-effort row/file count for a stage result."""

    if isinstance(result, Path):
        if result.suffix == ".parquet":
            return len(pd.read_parquet(result))
        return 0

    if isinstance(result, list):
        return len(result)

    if isinstance(result, dict):
        raw_path = result.get("raw_path")
        if not raw_path:
            return None

        path = Path(raw_path)
        if path.suffix != ".json" or not path.exists():
            return None

        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
        if isinstance(payload, list):
            return len(payload)
        return None

    return None


def resolve_rows_in(
    stage: PipelineRunStage,
    rows_by_stage: dict[PipelineRunStage, int | None],
) -> int | None:
    """Map each stage to its upstream row count."""

    if stage == PipelineRunStage.INGEST:
        return None
    if stage == PipelineRunStage.BRONZE:
        return rows_by_stage.get(PipelineRunStage.INGEST)
    if stage == PipelineRunStage.SILVER:
        return rows_by_stage.get(PipelineRunStage.BRONZE)
    if stage in (
        PipelineRunStage.GOLD_TEAM,
        PipelineRunStage.GOLD_INTERVAL,
    ):
        return rows_by_stage.get(PipelineRunStage.SILVER)
    if stage == PipelineRunStage.REPORTS:
        return None
    return None


def run_stage(
    *,
    run_id: str,
    match_id: int,
    rows_in: int | None,
    stage: PipelineRunStage,
    call: Callable[[], Any],
) -> tuple[PipelineRunStageRun, Any]:
    """Execute one stage with pinned inputs and record the outcome."""

    stage_run_id = new_stage_run_id()
    started_at = utc_now()

    set_stage_context(stage=stage)
    logger.info(
        "stage_started",
        extra={"stage_run_id": stage_run_id},
    )

    try:
        result = call()
        completed_at = utc_now()
        rows_out = count_output_rows(result)

        stage_run = write_stage_run(
            stage_run_id=stage_run_id,
            run_id=run_id,
            match_id=match_id,
            stage_name=stage,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.SUCCEEDED,
            rows_in=rows_in,
            rows_out=rows_out,
        )

        logger.info(
            "stage_succeeded",
            extra={
                "stage_run_id": stage_run_id,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "duration_seconds": stage_run.duration_seconds,
            },
        )
        return stage_run, result

    except Exception as error:
        completed_at = utc_now()

        stage_run = write_stage_run(
            stage_run_id=stage_run_id,
            run_id=run_id,
            match_id=match_id,
            stage_name=stage,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.FAILED,
            rows_in=rows_in,
            error_type=type(error).__name__,
            error_message=str(error),
        )

        logger.exception(
            "stage_failed",
            extra={
                "stage_run_id": stage_run_id,
                "rows_in": rows_in,
                "duration_seconds": stage_run.duration_seconds,
                "error_type": type(error).__name__,
            },
        )
        raise StageFailedError(
            stage=stage,
            error=error,
        ) from error

    finally:
        clear_stage_context()


def record_not_run_stages(
    run_id: str,
    match_id: int,
    failed_stage: PipelineRunStage,
) -> None:
    """Mark every later stage as not run after an earlier failure."""

    for stage in remaining_stages(failed_stage):
        write_stage_run(
            stage_run_id=new_stage_run_id(),
            run_id=run_id,
            match_id=match_id,
            stage_name=stage,
            started_at=None,
            completed_at=None,
            status=PipelineRunStatus.NOT_RUN,
        )
        logger.info(
            "stage_not_run",
            extra={"stage": stage},
        )


def run_pipeline(match_id: int) -> str:
    """Run ingest → bronze → silver → gold(+intervals) → reports."""

    run_id = new_run_id()
    started_at = utc_now()
    rows_by_stage: dict[PipelineRunStage, int | None] = {}

    set_run_context(run_id=run_id, match_id=match_id)
    logger.info("pipeline_started")

    try:
        stage_run, source = run_stage(
            run_id=run_id,
            match_id=match_id,
            rows_in=resolve_rows_in(
                PipelineRunStage.INGEST,
                rows_by_stage,
            ),
            stage=PipelineRunStage.INGEST,
            call=lambda: ingest_match(match_id),
        )
        rows_by_stage[PipelineRunStage.INGEST] = (
            stage_run.rows_out
        )

        stage_run, bronze_path = run_stage(
            run_id=run_id,
            match_id=match_id,
            rows_in=resolve_rows_in(
                PipelineRunStage.BRONZE,
                rows_by_stage,
            ),
            stage=PipelineRunStage.BRONZE,
            call=lambda: build_bronze(
                match_id=match_id,
                source=source,
            ),
        )
        rows_by_stage[PipelineRunStage.BRONZE] = (
            stage_run.rows_out
        )

        stage_run, silver_path = run_stage(
            run_id=run_id,
            match_id=match_id,
            rows_in=resolve_rows_in(
                PipelineRunStage.SILVER,
                rows_by_stage,
            ),
            stage=PipelineRunStage.SILVER,
            call=lambda: build_silver(
                match_id=match_id,
                bronze_path=bronze_path,
            ),
        )
        rows_by_stage[PipelineRunStage.SILVER] = (
            stage_run.rows_out
        )

        stage_run, gold_path = run_stage(
            run_id=run_id,
            match_id=match_id,
            rows_in=resolve_rows_in(
                PipelineRunStage.GOLD_TEAM,
                rows_by_stage,
            ),
            stage=PipelineRunStage.GOLD_TEAM,
            call=lambda: build_gold(
                match_id=match_id,
                silver_path=silver_path,
            ),
        )
        rows_by_stage[PipelineRunStage.GOLD_TEAM] = (
            stage_run.rows_out
        )

        stage_run, gold_intervals_path = run_stage(
            run_id=run_id,
            match_id=match_id,
            rows_in=resolve_rows_in(
                PipelineRunStage.GOLD_INTERVAL,
                rows_by_stage,
            ),
            stage=PipelineRunStage.GOLD_INTERVAL,
            call=lambda: build_gold_intervals(
                match_id=match_id,
                silver_path=silver_path,
            ),
        )
        rows_by_stage[PipelineRunStage.GOLD_INTERVAL] = (
            stage_run.rows_out
        )

        stage_run, _report_paths = run_stage(
            run_id=run_id,
            match_id=match_id,
            rows_in=resolve_rows_in(
                PipelineRunStage.REPORTS,
                rows_by_stage,
            ),
            stage=PipelineRunStage.REPORTS,
            call=lambda: generate_reports(
                match_id=match_id,
                silver_path=silver_path,
                gold_path=gold_path,
                gold_intervals_path=gold_intervals_path,
            ),
        )
        rows_by_stage[PipelineRunStage.REPORTS] = (
            stage_run.rows_out
        )

        completed_at = utc_now()
        run = write_pipeline_run(
            run_id=run_id,
            match_id=match_id,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.SUCCEEDED,
        )

        logger.info(
            "pipeline_succeeded",
            extra={
                "duration_seconds": run.duration_seconds,
            },
        )
        return run_id

    except StageFailedError as error:
        completed_at = utc_now()

        run = write_pipeline_run(
            run_id=run_id,
            match_id=match_id,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.FAILED,
            failed_stage=error.stage,
            error_type=type(error.error).__name__,
            error_message=str(error.error),
        )

        record_not_run_stages(
            run_id=run_id,
            match_id=match_id,
            failed_stage=error.stage,
        )

        logger.error(
            "pipeline_failed",
            extra={
                "duration_seconds": run.duration_seconds,
                "failed_stage": error.stage,
                "error_type": type(error.error).__name__,
                "error_message": str(error.error),
            },
        )
        raise


class StageFailedError(Exception):
    def __init__(
        self,
        stage: PipelineRunStage,
        error: Exception,
    ):
        self.stage = stage
        self.error = error
        super().__init__(
            f"Stage {stage} failed: {error}"
        )


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )
    args = parser.parse_args()

    try:
        run_pipeline(match_id=args.match_id)
    except Exception:
        logger.exception("pipeline_aborted")
        sys.exit(1)


if __name__ == "__main__":
    main()
