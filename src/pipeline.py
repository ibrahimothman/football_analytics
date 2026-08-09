from __future__ import annotations

from typing import Callable, Any
from pathlib import Path
import argparse
import sys
import logging

import pandas as pd

from src.observability.logging_config import (
    configure_logging,
    set_run_context,
    set_stage_context,
    clear_stage_context,
)

from src.metadata.run_tracker import (
    PipelineRunStage,
    PipelineRunStatus,
    remaining_stages,
    utc_now,
    new_run_id,
    new_stage_run_id,
    write_pipeline_run,
    write_stage_run,
)
from src.ingest_match import ingest_match
from src.build_bronze import build_bronze
from src.build_silver import build_silver
from src.build_gold import build_gold
from src.build_gold_intervals import build_gold_intervals
from src.generate_reports import generate_reports


logger = logging.getLogger(__name__)

STAGE_FUNCTIONS: dict[PipelineRunStage, Callable[[int], Any]] = {
    PipelineRunStage.INGEST: ingest_match,
    PipelineRunStage.BRONZE: build_bronze,
    PipelineRunStage.SILVER: build_silver,
    PipelineRunStage.GOLD_TEAM: build_gold,
    PipelineRunStage.GOLD_INTERVAL: build_gold_intervals,
    PipelineRunStage.REPORTS: generate_reports,
}


def count_output_rows(result: Any) -> int | None:
    if isinstance(result, Path):
        if result.suffix == ".parquet":
            return len(pd.read_parquet(result))
        return 0

    if isinstance(result, list):
        return len(result)

    return None


def run_stage(
    run_id: str,
    match_id: int,
    stage: PipelineRunStage,
    function: Callable[[int], Any],
) -> Any:
    stage_run_id = new_stage_run_id()
    started_at = utc_now()

    set_stage_context(stage=stage)
    logger.info("stage_started", extra={"stage_run_id": stage_run_id})

    try:
        result = function(match_id)
        completed_at = utc_now()
        rows_out = count_output_rows(result)

        run_stage = write_stage_run(
            stage_run_id=stage_run_id,
            run_id=run_id,
            match_id=match_id,
            stage_name=stage,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.SUCCEEDED,
            rows_out=rows_out,
        )

        logger.info(
            "stage_succeeded", 
            extra={
                "stage_run_id": stage_run_id, 
                "rows_out": rows_out,
                "duration_seconds": run_stage.duration_seconds,
            },
        )

    except Exception as e:
        completed_at = utc_now()

        run_stage = write_stage_run(
            stage_run_id=stage_run_id,
            run_id=run_id,
            match_id=match_id,
            stage_name=stage,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.FAILED,
            error_type=type(e).__name__,
            error_message=str(e),
        )

        logger.exception(
            "stage_failed", 
            extra={
                "stage_run_id": stage_run_id,
                "duration_seconds": run_stage.duration_seconds,
                "error_type": type(e).__name__,
            },
        )
        raise StageFailedError(stage=stage, error=e) from e


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
        logger.info("stage_not_run", extra={"stage": stage})


def run_pipeline(
    match_id: int,
) -> str:
    """Run the selected pipeline stages for one match."""

    run_id = new_run_id()
    started_at = utc_now()

    set_run_context(run_id=run_id, match_id=match_id)
    logger.info("pipeline_started")

    try:
        for stage_name, function in STAGE_FUNCTIONS.items():
            result = run_stage(
                run_id=run_id,
                match_id=match_id,
                stage=stage_name,
                function=function,
            )

        completed_at = utc_now()
        run = write_pipeline_run(
            run_id=run_id,
            match_id=match_id,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.SUCCEEDED,
        )

        logger.info("pipeline_succeeded", extra={"duration_seconds": run.duration_seconds})

    except StageFailedError as e:
        completed_at = utc_now()

        run = write_pipeline_run(
            run_id=run_id,
            match_id=match_id,
            started_at=started_at,
            completed_at=completed_at,
            status=PipelineRunStatus.FAILED,
            failed_stage=e.stage,
            error_type=type(e.error).__name__,
            error_message=str(e.error),
        )


        record_not_run_stages(
            run_id=run_id,
            match_id=match_id,
            failed_stage=e.stage
        )

        logger.error(
            "pipeline_failed", 
            extra={
                "duration_seconds": run.duration_seconds, 
                "failed_stage": e.stage,
                "error_type": type(e.error).__name__,
                "error_message": str(e.error),
            }
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

    parser.add_argument("--match-id", type=int, required=True)

    args = parser.parse_args()

    try:
        run_pipeline(match_id=args.match_id)
    except Exception as e:
        sys.exit(1)


if __name__ == "__main__":
    main()
