import logging
from datetime import timedelta

import requests


import pendulum
from airflow.sdk import (
    dag,
    task, 
    Param, 
    get_current_context,
    RetryPolicy,
    RetryDecision,
    
)


from src.ingest_match import ingest_match
from src.build_bronze import build_bronze
from src.build_silver import build_silver
from src.build_gold import build_gold
from src.build_gold_intervals import build_gold_intervals
from src.generate_reports import generate_reports
from src.observability.airflow_callbacks import dag_failure_callback, task_failure_callback, task_retry_callback
from src.observability.stage_observer import observe_stage
from src.storage.storage_store import count_parquet_rows


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
    max_active_runs=5,
    on_failure_callback=dag_failure_callback,
    default_args={
        "on_failure_callback": task_failure_callback,
        "on_retry_callback": task_retry_callback,
    },
    tags=["football"],
)
def football_match_pipeline():

    @task(
        task_id="ingest",
        pool="statsbomb_api",
        retries=3,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
        retry_policy=INGEST_RETRY_POLICY,
    )
    def ingest()-> dict:
        context = get_current_context()

        dag_run = context["dag_run"]
        conf_match_id = dag_run.conf.get("match_id") if dag_run and dag_run.conf else None

        match_id = conf_match_id if conf_match_id else context["params"]["match_id"]

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

            silver_uri = build_silver(
                match_id=artifact["match_id"],
                bronze_uri=bronze_uri,
            )

            metrics["rows_out"] = count_parquet_rows(
                uri=silver_uri,
            )

            return {
                **artifact,
                "silver_uri": silver_uri,
            }

    @task(task_id="gold")
    def gold(artifact: dict) -> str:
        silver_uri = artifact["silver_uri"]

        context = get_current_context()
        task_instance = context["ti"]

        with observe_stage(
            airflow_run_id=context["dag_run"].run_id,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            try_number=task_instance.try_number,
            match_id=artifact["match_id"],
            stage="GOLD_TEAM",
        ) as metrics:
            metrics["rows_in"] = count_parquet_rows(
                uri=silver_uri,
            )

            gold_uri = build_gold(
                match_id=artifact["match_id"],
                silver_uri=silver_uri,
            )

            metrics["rows_out"] = count_parquet_rows(
                uri=gold_uri,
            )

            return {
                **artifact,
                "gold_uri": gold_uri,
            }

    @task(task_id="gold_intervals")
    def gold_intervals(artifact: dict) -> str:
        silver_uri = artifact["silver_uri"]

        context = get_current_context()
        task_instance = context["ti"]

        with observe_stage(
            airflow_run_id=context["dag_run"].run_id,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            try_number=task_instance.try_number,
            match_id=artifact["match_id"],
            stage="GOLD_INTERVAL",
        ) as metrics:
            metrics["rows_in"] = count_parquet_rows(
                uri=silver_uri,
            )

            gold_intervals_uri = build_gold_intervals(
                match_id=artifact["match_id"],
                silver_uri=silver_uri,
            )

            metrics["rows_out"] = count_parquet_rows(
                uri=gold_intervals_uri,
            )

            return {
                **artifact,
                "gold_intervals_uri": gold_intervals_uri,
            }

    @task(task_id="reports")
    def reports(team_artifact: dict, intervals_artifact: dict) -> int:
        """Generate reports after both gold tasks complete."""

        if (
            team_artifact["match_id"]
            != intervals_artifact["match_id"]
        ):
            raise ValueError(
                "Gold artifacts belong to different matches."
            )

        if (
            team_artifact["file_hash"]
            != intervals_artifact["file_hash"]
        ):
            raise ValueError(
                "Gold artifacts were produced from "
                "different source versions."
            )

        context = get_current_context()
        task_instance = context["ti"]

        with observe_stage(
            airflow_run_id=context["dag_run"].run_id,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            try_number=task_instance.try_number,
            match_id=team_artifact["match_id"],
            stage="REPORTS",
        ):
            reports_paths = generate_reports(
                match_id=team_artifact["match_id"],
                silver_uri=team_artifact["silver_uri"],
                gold_uri=team_artifact["gold_uri"],
                gold_intervals_uri=intervals_artifact[
                    "gold_intervals_uri"
                ],
            )

            return {
                **team_artifact,
                **intervals_artifact,
                "reports_paths": [
                    str(path) for path in reports_paths
                ],
            }

    ingested_match_id = ingest()
    bronze_artifact = bronze(ingested_match_id)
    silver_artifact = silver(bronze_artifact)
    gold_team_artifact = gold(silver_artifact)
    gold_intervals_artifact = gold_intervals(silver_artifact)
    reports(gold_team_artifact, gold_intervals_artifact)


football_match_pipeline()
