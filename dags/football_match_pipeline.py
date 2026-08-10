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
        retries=3,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
        retry_policy=INGEST_RETRY_POLICY,
    )
    def ingest():
        context = get_current_context()
        match_id = context["params"]["match_id"]

        logger.info(f"Starting to ingest match {match_id} from StatsBomb Open Data")


        result = ingest_match(match_id)

        logger.info(f"Finished ingestion for match {match_id}")

        return match_id

    @task(task_id="bronze")
    def bronze(match_id: int) -> str:

        logger.info(f"Starting to build bronze for match {match_id}")

        bronze_output_path = build_bronze(match_id)

        logger.info(f"Finished building bronze for match {match_id}")

        return match_id

    @task(task_id="silver")
    def silver(match_id: int) -> str:

        logger.info(f"Starting to build silver for match {match_id}")

        silver_output_path = build_silver(match_id)

        logger.info(f"Finished building silver for match {match_id}")

        return match_id

    @task(task_id="gold")
    def gold(match_id: int) -> str:

        logger.info(f"Starting to build gold team for match {match_id}")

        gold_output_path = build_gold(match_id)

        logger.info(f"Finished building gold team for match {match_id}")

        return match_id

    @task(task_id="gold_intervals")
    def gold_intervals(match_id: int) -> str:
        logger.info(f"Starting to build gold intervals for match {match_id}")

        gold_intervals_output_path = build_gold_intervals(match_id)

        logger.info(f"Finished building gold intervals for match {match_id}")

        return match_id

    @task(task_id="reports")
    def reports(team_match_id: int, interval_match_id: int) -> int:
        """Generate reports after both gold tasks complete."""

        if team_match_id != interval_match_id:
            raise ValueError(f"Team match {team_match_id} and interval match {interval_match_id} do not match")

        logger.info(f"Starting to generate reports for match {team_match_id}")

        generate_reports(team_match_id)

        logger.info(f"Finished generating reports for match {team_match_id}")

        return team_match_id

    ingested_match_id = ingest()
    bronze_match_id = bronze(ingested_match_id)
    silver_match_id = silver(bronze_match_id)
    gold_team_match_id = gold(silver_match_id)
    gold_intervals_match_id = gold_intervals(silver_match_id)
    reports(gold_team_match_id, gold_intervals_match_id)


football_match_pipeline()
