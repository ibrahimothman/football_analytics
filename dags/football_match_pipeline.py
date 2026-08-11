import logging
from datetime import timedelta
from pathlib import Path

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

        logger.info(
            "Starting to build bronze for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        bronze_path = build_bronze(
            match_id=artifact["match_id"],
            file_hash=artifact["file_hash"],
        )

        logger.info(
            "Finished building bronze for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        return {
            **artifact,
            "bronze_path": str(bronze_path),
        }

    @task(task_id="silver")
    def silver(artifact: dict) -> str:

        logger.info(
            "Starting to build silver for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        silver_path = build_silver(
            match_id=artifact["match_id"],
            bronze_path=Path(artifact["bronze_path"]),
        )

        logger.info(
            "Finished building silver for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        return {
            **artifact,
            "silver_path": str(silver_path),
        }

    @task(task_id="gold")
    def gold(artifact: dict) -> str:

        logger.info(
            "Starting to build gold team for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        gold_path = build_gold(
            match_id=artifact["match_id"],
            silver_path=Path(artifact["silver_path"]),
        )

        logger.info(
            "Finished building gold team for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        return {
            **artifact,
            "gold_path": str(gold_path),
        }

    @task(task_id="gold_intervals")
    def gold_intervals(artifact: dict) -> str:
        logger.info(
            "Starting to build gold intervals for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        gold_intervals_path = build_gold_intervals(
            match_id=artifact["match_id"],
            silver_path=Path(artifact["silver_path"]),
        )

        logger.info(
            "Finished building gold intervals for match %s",
            str(artifact["match_id"])+artifact["file_hash"],
        )

        return {
            **artifact,
            "gold_intervals_path": str(gold_intervals_path),
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

        logger.info(f"Starting to generate reports for match {team_artifact['match_id']}")

        reports_paths = generate_reports(
            match_id=team_artifact["match_id"],
            silver_path=Path(team_artifact["silver_path"]),
            gold_path=Path(team_artifact["gold_path"]),
            gold_intervals_path=Path(intervals_artifact["gold_intervals_path"]),
        )

        return {
            **team_artifact,
            **intervals_artifact,
            "reports_paths": [str(path) for path in reports_paths],
        }

    ingested_match_id = ingest()
    bronze_artifact = bronze(ingested_match_id)
    silver_artifact = silver(bronze_artifact)
    gold_team_artifact = gold(silver_artifact)
    gold_intervals_artifact = gold_intervals(silver_artifact)
    reports(gold_team_artifact, gold_intervals_artifact)


football_match_pipeline()
