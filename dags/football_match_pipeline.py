import logging
from datetime import timedelta

import pendulum
from airflow.sdk import (
    dag,
    task, 
    Param, 
    get_current_context,
    RetryPolicy,
    RetryDecision,
    
)

from airflow.providers.standard.operators.bash import BashOperator

from src.observability.airflow_callbacks import dag_failure_callback, task_failure_callback, task_retry_callback



logger = logging.getLogger("airflow.task")


class IngestRetryPolicy(RetryPolicy):
    """Retry only ingestion failures likely to be transient."""

    def evaluate(
        self,
        exception,
        try_number,
        max_tries,
        context=None,
    ):
        import requests

        # Network timeout
        if isinstance(
            exception,
            requests.exceptions.Timeout,
        ):
            return RetryDecision.retry(
                retry_delay=timedelta(
                    seconds=30
                )
            )

        # Connection interrupted / unavailable
        if isinstance(
            exception,
            requests.exceptions.ConnectionError,
        ):
            return RetryDecision.retry(
                retry_delay=timedelta(
                    seconds=30
                )
            )

        # HTTP response errors
        if isinstance(
            exception,
            requests.exceptions.HTTPError,
        ):
            response = exception.response

            if response is None:
                return RetryDecision.fail(
                    reason="HTTP error without response"
                )

            status = response.status_code

            # Rate limiting
            if status == 429:
                retry_after = int(
                    response.headers.get(
                        "Retry-After",
                        60,
                    )
                )

                return RetryDecision.retry(
                    retry_delay=timedelta(
                        seconds=retry_after
                    )
                )

            # Provider/server problem
            if 500 <= status < 600:
                return RetryDecision.retry(
                    retry_delay=timedelta(
                        seconds=60
                    )
                )

            # 4xx: our request is probably wrong
            if 400 <= status < 500:
                return RetryDecision.fail(
                    reason=(
                        f"Non-retryable HTTP "
                        f"status {status}"
                    )
                )

        # Unknown exception:
        # fail rather than blindly retrying bugs.
        return RetryDecision.fail(
            reason=(
                f"Non-retryable exception: "
                f"{type(exception).__name__}"
            )
        )


INGEST_RETRY_POLICY = (IngestRetryPolicy())    

@dag(
    dag_id="football_match_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    params={
        "match_id": Param(
            type="integer",
            title="StatsBomb Match ID",
            description="Match ID to ingest from StatsBomb Open Data",
            minimum=1,
        ),
    },
    max_active_runs=1,
    max_active_tasks=1, 
    on_failure_callback=dag_failure_callback,
    default_args={
        "on_failure_callback": task_failure_callback,
        "on_retry_callback": task_retry_callback,
    },
    tags=["football"],
)
def football_match_pipeline():

    @task(task_id="resolve_match_id")
    def resolve_match_id() -> int:
        context = get_current_context()
        dag_run = context["dag_run"]
        conf_match_id = (
            dag_run.conf.get("match_id")
            if dag_run and dag_run.conf
            else None
        )
        return conf_match_id or context["params"]["match_id"]

    @task(
        task_id="ingest",
        pool="statsbomb_api",
        retries=3,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
        retry_policy=INGEST_RETRY_POLICY,
    )
    def ingest(match_id: int) -> dict:
        from src.ingest_match import ingest_match

        logger.info(f"Starting to ingest match {match_id} from StatsBomb Open Data")
        artifact = ingest_match(match_id)

        logger.info(
            "Source artifact selected"
            "match_id: %s, source_version: %s, hash: %s",
            artifact["match_id"],
            artifact["source_version"],
            artifact["file_hash"],

        )

        return artifact

    @task(task_id="bronze")
    def bronze(artifact: dict) -> dict:
        from src.build_bronze import build_bronze
        from src.observability.stage_observer import observe_stage
        from src.storage.storage_store import count_parquet_rows

        context = get_current_context()
        task_instance = context["ti"]

        with observe_stage(
            airflow_run_id=context["dag_run"].run_id,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            try_number=task_instance.try_number,
            match_id=artifact["match_id"],
            stage="BRONZE",
        ) as metrics:
            bronze_uri = build_bronze(
                match_id=artifact["match_id"],
                source=artifact,
            )

            metrics["rows_out"] = count_parquet_rows(
                uri=bronze_uri,
            )

            return {
                **artifact,
                "bronze_uri": bronze_uri,
            }

    @task(task_id="silver")
    def silver(artifact: dict) -> str:
        from src.build_silver import build_silver
        from src.observability.stage_observer import observe_stage

        bronze_uri = artifact["bronze_uri"]

        context = get_current_context()
        task_instance = context["ti"]

        with observe_stage(
            airflow_run_id=context["dag_run"].run_id,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            try_number=task_instance.try_number,
            match_id=artifact["match_id"],
            stage="SILVER",
        ) as metrics:

            result = build_silver(
                match_id=artifact["match_id"],
                bronze_uri=bronze_uri,
            )

            return {
                **artifact,
                "snapshot_id": result["snapshot_id"],
            }
            
    @task(task_id="load_silver_events_to_db")
    def load_silver_events_to_db(artifact: dict) -> None:
        from src.serving.load_silver_events import load_silver_to_postgres

        snapshot_id = artifact["snapshot_id"]
        match_id = artifact["match_id"]
        load_silver_to_postgres(match_id, snapshot_id)


    build_dbt = BashOperator(
        task_id="build_dbt",
        pool="dbt_writes",
        cwd="/opt/airflow/dbt/football_analytics",
        bash_command="""
            dbt build \
            --select stg_silver_events \
            --target docker \
            --profiles-dir /opt/airflow/dbt/football_analytics \
            --vars '{"match_id": {{ ti.xcom_pull(task_ids="resolve_match_id") }}}'
        """,
    )    

    build_gold_facts = BashOperator(
        task_id="build_gold_facts",
        pool="dbt_writes",
        cwd="/opt/airflow/dbt/football_analytics",
        bash_command="""
            dbt build \
            --select fact_gold_team fact_gold_intervals \
            --target docker \
            --profiles-dir /opt/airflow/dbt/football_analytics \
            --vars '{"match_id": {{ ti.xcom_pull(task_ids="resolve_match_id") }}}'
        """,
    )


    @task(task_id="reports")
    def reports() -> int:
        """Generate reports after both gold tasks complete."""
        from src.generate_reports import generate_reports
        from src.observability.stage_observer import observe_stage

        context = get_current_context()
        task_instance = context["ti"]

        match_id = context["ti"].xcom_pull(task_ids="resolve_match_id")

        with observe_stage(
            airflow_run_id=context["dag_run"].run_id,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            try_number=task_instance.try_number,
            match_id=match_id,
            stage="REPORTS",
        ):
            reports_paths = generate_reports(match_id)

            return {
                "reports_paths": [
                    str(path) for path in reports_paths
                ],
            }

   

    match_id = resolve_match_id()
    artifact = ingest(match_id)
    bronze_artifact = bronze(artifact)
    silver_artifact = silver(bronze_artifact)
    loaded_silver_events = load_silver_events_to_db(silver_artifact)
    loaded_silver_events >> build_dbt >> build_gold_facts >> reports()

football_match_pipeline()
