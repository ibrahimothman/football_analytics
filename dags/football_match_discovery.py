import logging

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from src.metadata.manifest import load_ingested_match_ids


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

    @task(task_id="discover_matches")
    def discover_matches() -> list[dict]:
        """Discover new matches to process."""

        from src.discover_matches import get_matches

        ingested_match_ids = load_ingested_match_ids()
        available_matches = get_matches(
            team_name="Arsenal",
        )["match_id"].tolist()

        new_matches = [
            match_id
            for match_id in available_matches
            if match_id not in ingested_match_ids
        ]

        logger.info(
            "Discovered %s matches to process. "
            "Unprocessed matches: %s",
            len(available_matches),
            len(new_matches),
        )

        logger.info(
            "New match IDs:\n%s",
            "\n".join(
                str(match_id) for match_id in new_matches
            ),
        )

        return [
            {
                "conf": {
                    "match_id": match_id,
                },
                "trigger_run_id": f"match_{match_id}",
            }
            for match_id in new_matches[:5]
        ]

    new_match_configs = discover_matches()

    TriggerDagRunOperator.partial(
        task_id="trigger_match_pipeline",
        trigger_dag_id="football_match_pipeline",
        wait_for_completion=False,
    ).expand_kwargs(
        new_match_configs
    )


football_match_discovery()
