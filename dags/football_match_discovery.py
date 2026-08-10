import logging
from pathlib import Path
import json


import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from src.discover_matches import get_matches

logger = logging.getLogger("airflow.task")


MANIFEST_PATH = Path("data/metadata/ingestion_manifest.jsonl")

def get_ingested_match_ids() -> set[int]:
    """Get the last ingested match ID from the database."""
    if not MANIFEST_PATH.exists():
        return set()

    ingested_match_ids = set()
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        for line in f:
            match_id = json.loads(line)["match_id"]
            ingested_match_ids.add(int(match_id))
    return ingested_match_ids
        


@dag(
    dag_id="football_match_discovery",
    schedule="@daily",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=["football", "discovery"],
)
def football_match_discovery()->list[int]:

    @task(task_id="discover_matches")
    def discover_matches() -> None:
        """Discover new matches to process."""
        ingested_match_ids = get_ingested_match_ids()
        available_matches = get_matches()["match_id"].tolist()
        new_matches = [
            match_id
            for match_id in available_matches
            if match_id not in ingested_match_ids
        ]

        logger.info(f"Discovered {len(available_matches)} matches to process.\
        Unprocessed matches: {len(new_matches)}")

        logger.info(
            "New match IDs:\n%s",
            "\n".join(str(match_id) for match_id in new_matches),
            )
        return [
            {
                "match_id": match_id
            }
            for match_id in new_matches[:2]
        ]

    new_match_configs = (
        discover_matches()
    )

    TriggerDagRunOperator.partial(
        task_id="trigger_match_pipeline",

        trigger_dag_id=(
            "football_match_pipeline"
        ),

        wait_for_completion=False,
    ).expand(
        conf=new_match_configs
    )


football_match_discovery()
