from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EVENTS_PATH = Path(
    "data/metadata/airflow_events.jsonl"
)


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def append_event(
    event: dict[str, Any],
) -> None:

    EVENTS_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with EVENTS_PATH.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(
                event,
                default=str,
            )
            + "\n"
        )

def task_retry_callback(
    context: dict[str, Any],
) -> None:

    task_instance = context[
        "task_instance"
    ]

    exception = context.get(
        "exception"
    )

    event = {
        "event_type": "TASK_RETRY",
        "occurred_at": utc_now(),

        "dag_id": (
            task_instance.dag_id
        ),

        "task_id": (
            task_instance.task_id
        ),

        "airflow_run_id": (
            context.get("run_id")
        ),

        "match_id": (
            context
            .get("params", {})
            .get("match_id")
        ),

        "try_number": (
            task_instance.try_number
        ),

        "exception_type": (
            type(exception).__name__
            if exception
            else None
        ),

        "error_message": (
            str(exception)
            if exception
            else None
        ),
    }

    append_event(event)        

def task_failure_callback(
    context: dict[str, Any],
) -> None:

    task_instance = context[
        "task_instance"
    ]

    exception = context.get(
        "exception"
    )

    event = {
        "event_type": "TASK_FAILED",
        "occurred_at": utc_now(),

        "dag_id": (
            task_instance.dag_id
        ),

        "task_id": (
            task_instance.task_id
        ),

        "airflow_run_id": (
            context.get("run_id")
        ),

        "match_id": (
            context
            .get("params", {})
            .get("match_id")
        ),

        "try_number": (
            task_instance.try_number
        ),

        "exception_type": (
            type(exception).__name__
            if exception
            else None
        ),

        "error_message": (
            str(exception)
            if exception
            else None
        ),
    }

    append_event(event)    


def dag_failure_callback(
    context: dict[str, Any],
) -> None:

    event = {
        "event_type": "DAG_FAILED",
        "occurred_at": utc_now(),

        "dag_id": (
            context["dag"].dag_id
        ),

        "airflow_run_id": (
            context.get("run_id")
        ),

        "match_id": (
            context
            .get("params", {})
            .get("match_id")
        ),
    }

    append_event(event)    