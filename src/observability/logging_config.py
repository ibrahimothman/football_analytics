from pathlib import Path
import logging
from datetime import datetime, timezone
import json
from contextvars import ContextVar
import os
import sys

from src.config.settings import (
    LOG_DIR,
    LOG_LEVEL,
    PIPELINE_LOG_PATH,
)


LOG_PATH = PIPELINE_LOG_PATH


RUN_ID = ContextVar("run_id", default=None)
MATCH_ID = ContextVar("match_id", default=None)
STAGE = ContextVar("stage", default=None)



STANDARD_FIELDS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """Format Python logs as JSON."""

    def format(
        self,
        record: logging.LogRecord,
    ) -> str:

        timestamp = datetime.fromtimestamp(
            record.created,
            tz=timezone.utc,
        ).isoformat()

        payload = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": RUN_ID.get(),
            "match_id": MATCH_ID.get(),
            "stage": STAGE.get(),
        }

        # Include extra fields supplied using:
        #
        # logger.info(
        #     "...",
        #     extra={"rows_out": 3000},
        # )
        for key, value in record.__dict__.items():

            if (
                key not in STANDARD_FIELDS
                and key not in payload
            ):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = (
                self.formatException(
                    record.exc_info
                )
            )

        return json.dumps(
            payload,
            default=str,
        )

def configure_logging():
    """Configure logging for the pipeline."""
    
    root_logger = logging.getLogger()

    if getattr(root_logger, "_football_pipeline_configured", False):
        return

    root_logger.setLevel(LOG_LEVEL)

    formatter = JsonFormatter()

    console_handler = logging.StreamHandler(
        stream=sys.stdout,
    )

    console_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(
        LOG_PATH,
        encoding="utf-8"
    )

    file_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)

    root_logger._football_pipeline_configured = True


def set_run_context(run_id: str, match_id: int) -> None:
    """Set the run and match context for logging."""
    RUN_ID.set(run_id)
    MATCH_ID.set(match_id)

def set_stage_context(stage: str) -> None:
    """Set the stage context for logging."""
    STAGE.set(stage)

def clear_stage_context() -> None:
    """Clear the stage context for logging."""
    STAGE.set(None)
