import pendulum
from airflow.sdk import (
    dag,
    task,
    Param,
    get_current_context,
)
from airflow.providers.standard.operators.bash import BashOperator


from src.observability.stage_observer import observe_stage
from src.generate_reports import generate_reports
from src.observability.airflow_callbacks import (
    dag_failure_callback,
    task_failure_callback,
)


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

    build_silver_events_dbt = BashOperator(
        task_id="build_silver_events_dbt",
        pool="dbt_writes",
        cwd="/opt/airflow/dbt/football_analytics",
        bash_command="""
            dbt build \
            --select stg_silver_events+ \
            --target docker \
            --profiles-dir /opt/airflow/dbt/football_analytics \
            --vars '{"match_id": {{ params.match_id }}}'
        """,
    )

    @task(task_id="reports")
    def reports() -> int:
        """Generate reports after both gold tasks complete."""

        context = get_current_context()
        task_instance = context["ti"]

        match_id = context["params"]["match_id"]

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

    build_silver_events_dbt >> reports()


football_gold_rebuild()
