import logging
from pathlib import Path

import pandas as pd
import pendulum
from airflow.sdk import (
    dag,
    task,
    Param,
    get_current_context,
)

from src.build_gold import build_gold
from src.build_gold_intervals import build_gold_intervals
from src.generate_reports import generate_reports
from src.paths import latest_file
from src.observability.airflow_callbacks import (
    dag_failure_callback,
    task_failure_callback,
)

logger = logging.getLogger("airflow.task")

SILVER_DIR = Path("data/silver")


@dag(
    dag_id="football_gold_rebuild",
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
            description=(
                "Match ID whose gold metrics and "
                "reports should be rebuilt from silver"
            ),
            minimum=1,
        ),
    },
    max_active_runs=2,
    on_failure_callback=dag_failure_callback,
    default_args={
        "on_failure_callback": task_failure_callback,
    },
    tags=["football", "gold", "rebuild"],
)
def football_gold_rebuild():

    @task(task_id="resolve_silver")
    def resolve_silver() -> dict:
        """Resolve the latest silver artifact for the match."""

        context = get_current_context()
        dag_run = context["dag_run"]
        conf_match_id = (
            dag_run.conf.get("match_id")
            if dag_run and dag_run.conf
            else None
        )
        match_id = int(
            conf_match_id
            if conf_match_id is not None
            else context["params"]["match_id"]
        )

        silver_path = latest_file(
            SILVER_DIR / f"match_id={match_id}",
            "events_*.parquet",
        )

        return {
            "match_id": match_id,
            "silver_path": str(silver_path),
        }

    @task(task_id="gold_team")
    def gold_team(artifact: dict) -> dict:
        logger.info(
            "Starting to rebuild gold team for match %s",
            str(artifact["match_id"]),
        )

        gold_path = build_gold(
            match_id=artifact["match_id"],
            silver_path=Path(artifact["silver_path"]),
        )

        logger.info(
            "Finished rebuilding gold team for match %s",
            str(artifact["match_id"])   ,
        )

        return {
            **artifact,
            "gold_path": str(gold_path),
        }

    @task(task_id="gold_intervals")
    def gold_intervals(artifact: dict) -> dict:
        logger.info(
            "Starting to rebuild gold intervals for match %s",
            str(artifact["match_id"])   ,
        )

        gold_intervals_path = build_gold_intervals(
            match_id=artifact["match_id"],
            silver_path=Path(artifact["silver_path"]),
        )

        logger.info(
            "Finished rebuilding gold intervals for match %s",
            str(artifact["match_id"])   ,
        )

        return {
            **artifact,
            "gold_intervals_path": str(gold_intervals_path),
        }

    @task(task_id="reports")
    def reports(
        team_artifact: dict,
        intervals_artifact: dict,
    ) -> dict:
        """Generate reports after both gold tasks complete."""

        if (
            team_artifact["silver_path"]
            != intervals_artifact["silver_path"]
        ):
            raise ValueError(
                "Gold branches were built "
                "from different Silver artifacts."
            )
                

        logger.info(
            "Starting to generate reports for match %s",
            team_artifact["match_id"],
        )

        reports_paths = generate_reports(
            match_id=team_artifact["match_id"],
            silver_path=Path(team_artifact["silver_path"]),
            gold_path=Path(team_artifact["gold_path"]),
            gold_intervals_path=Path(
                intervals_artifact["gold_intervals_path"]
            ),
        )

        logger.info(
            "Finished generating reports for match %s",
            team_artifact["match_id"],
        )

        return {
            **team_artifact,
            **intervals_artifact,
            "reports_paths": [
                str(path) for path in reports_paths
            ],
        }

    silver_artifact = resolve_silver()
    gold_team_artifact = gold_team(silver_artifact)
    gold_intervals_artifact = gold_intervals(silver_artifact)
    reports(gold_team_artifact, gold_intervals_artifact)


football_gold_rebuild()
