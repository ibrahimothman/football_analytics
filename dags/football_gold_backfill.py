import logging

import pendulum
from airflow.sdk import (
    dag,
    task,
    Param,
    get_current_context,
)
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from src.observability.airflow_callbacks import (
    dag_failure_callback,
    task_failure_callback,
)

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="football_gold_backfill",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    params={
        "match_ids": Param(
            type="array",
            title="StatsBomb Match IDs",
            description=(
                "Non-empty list of match IDs to "
                "rebuild gold metrics and reports for"
            ),
            items={"type": "integer", "minimum": 1},
            minItems=1,
        ),
    },
    max_active_runs=1,
    on_failure_callback=dag_failure_callback,
    default_args={
        "on_failure_callback": task_failure_callback,
    },
    tags=["football", "gold", "backfill"],
)
def football_gold_backfill():

    @task(task_id="prepare_rebuild_triggers")
    def prepare_rebuild_triggers() -> list[dict]:
        """Build TriggerDagRun configs for each match_id."""

        context = get_current_context()

        match_ids = [
            int(match_id) for match_id in context["params"]["match_ids"]
        ]


        return [
            {
                "conf": {
                    "match_id": match_id,
                },
            }
            for match_id in match_ids
        ]

    rebuild_configs = prepare_rebuild_triggers()

    TriggerDagRunOperator.partial(
        task_id="trigger_gold_rebuild",
        trigger_dag_id="football_gold_rebuild",
        wait_for_completion=False,
    ).expand_kwargs(
        rebuild_configs
    )


football_gold_backfill()
