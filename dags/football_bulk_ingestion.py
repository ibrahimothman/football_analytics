import logging
from datetime import timedelta

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

from src.discover_matches import get_matches
from src.metadata.manifest import load_ingested_match_ids
from src.observability.airflow_callbacks import (
    dag_failure_callback,
    task_failure_callback,
)


logger = logging.getLogger("airflow.task")


@dag(
    dag_id="football_bulk_ingestion",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    params={
        "competition_id": Param(
            type="integer",
            title="StatsBomb Competition ID",
            description="Competition to ingest matches from",
            minimum=1,
        ),
        "season_id": Param(
            type="integer",
            title="StatsBomb Season ID",
            description="Season to ingest matches from",
            minimum=1,
        ),
        "team_name": Param(
            default=None,
            type=["string", "null"],
            title="Team Name",
            description=(
                "Optional team filter. When set, only matches "
                "where the team is home or away are included."
            ),
        ),
    },
    max_active_runs=1,
    on_failure_callback=dag_failure_callback,
    default_args={
        "on_failure_callback": task_failure_callback,
    },
    tags=["football", "ingestion", "bulk"],
)
def football_bulk_ingestion():

    @task(
        task_id="resolve_scope",
        retries=3,
        retry_delay=timedelta(seconds=10),
    )
    def resolve_scope() -> list[dict]:
        """Resolve competition/season/team scope into pipeline triggers."""

        context = get_current_context()
        params = context["params"]

        competition_id = int(params["competition_id"])
        season_id = int(params["season_id"])
        team_name = params.get("team_name") or None

        if isinstance(team_name, str):
            team_name = team_name.strip() or None

        matches = get_matches(
            competition_id=competition_id,
            season_id=season_id,
            team_name=team_name,
        )

        ingested_match_ids = load_ingested_match_ids()

        new_match_ids = [
            int(match_id)
            for match_id in matches["match_id"].tolist()
            if int(match_id) not in ingested_match_ids
        ]

        logger.info(
            "Bulk scope competition_id=%s season_id=%s "
            "team_name=%s available=%s already_ingested=%s new=%s",
            competition_id,
            season_id,
            team_name,
            len(matches),
            len(matches) - len(new_match_ids),
            len(new_match_ids),
        )

        if not new_match_ids:
            logger.info(
                "No new matches to ingest for the selected scope."
            )
            return []

        logger.info(
            "New match IDs:\n%s",
            "\n".join(str(match_id) for match_id in new_match_ids),
        )

        return [
            {
                "conf": {
                    "match_id": match_id,
                },
                "trigger_run_id": (
                    f"bulk_match_{match_id}"
                ),
            }
            for match_id in new_match_ids
        ]

    trigger_configs = resolve_scope()

    TriggerDagRunOperator.partial(
        task_id="trigger_match_pipeline",
        trigger_dag_id="football_match_pipeline",
        wait_for_completion=False,
        skip_when_already_exists=True,
    ).expand_kwargs(
        trigger_configs
    )


football_bulk_ingestion()
