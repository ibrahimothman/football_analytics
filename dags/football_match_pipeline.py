import logging

import pendulum
from airflow.sdk import dag, task, Param, get_current_context

from src.ingest_match import ingest_match
from src.build_bronze import build_bronze
from src.build_silver import build_silver
from src.build_gold import build_gold
from src.build_gold_intervals import build_gold_intervals
from src.generate_reports import generate_reports

logger = logging.getLogger("airflow.task")

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
    tags=["football"],
)
def football_match_pipeline():

    @task(task_id="ingest")
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
