import json
import logging

import pandas as pd
import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.providers.standard.operators.bash import BashOperator

from src.build_dim_match import build_dim_match
from src.metadata.manifest import load_ingested_match_ids
from src.serving.load_dim_match import upsert_dim_match


logger = logging.getLogger("airflow.task")


@dag(
    dag_id="football_match_discovery",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=["football", "discovery"],
)
def football_match_discovery():

    @task(task_id="build_dim_match")
    def build_dim_match_task() -> list[dict]:
        dim_matches_df = build_dim_match()
        upsert_dim_match(dim_matches_df)

        logger.info(
            "Built dim_match with %s rows",
            len(dim_matches_df),
        )

        return dim_matches_df["match_id"].tolist()

    run_dbt = BashOperator(
        task_id="run_dbt_stg_dim_match",
        pool="dbt_writes",
        bash_command="""
            cd /opt/airflow/dbt/football_analytics &&
            dbt run --select stg_dim_match --target docker --profiles-dir /opt/airflow/dbt/football_analytics
        """,
    )


    @task(task_id="find_new_matches")
    def find_new_matches(
        dim_match_ids: list[int],
    ) -> list[dict]:
        ingested_match_ids = load_ingested_match_ids()

        new_match_ids = [
            match_id
            for match_id in dim_match_ids
            if match_id
            not in ingested_match_ids
        ]

        logger.info(
            "Discovered %s matches to process. "
            "Unprocessed matches: %s",
            len(dim_match_ids),
            len(new_match_ids),
        )

        logger.info(
            "New match IDs:\n%s",
            "\n".join(
                str(match_id)
                for match_id in new_match_ids
            ),
        )

        return [
            {
                "conf": {
                    "match_id": match_id,
                },
                "trigger_run_id": f"match_{match_id}",
            }
            for match_id in new_match_ids[:5]
        ]

    dim_match_ids = build_dim_match_task()
    dim_match_ids >> run_dbt
    new_match_configs = find_new_matches(
        dim_match_ids,
    )

    TriggerDagRunOperator.partial(
        task_id="trigger_match_pipeline",
        trigger_dag_id="football_match_pipeline",
        wait_for_completion=False,
    ).expand_kwargs(
        new_match_configs
    )


football_match_discovery()
