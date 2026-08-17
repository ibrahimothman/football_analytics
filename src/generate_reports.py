"""Generate match reports from Silver and Gold data."""

from __future__ import annotations

import argparse
import logging


import pandas as pd

from src.storage.database import (
    get_db_connection,
)

from src.reports.match_summary import (
    generate_match_summary,
)
from src.reports.shot_map import (
    generate_shot_map,
)
from src.reports.xg_timeline import (
    generate_xg_timeline,
)
from src.reports.xt_momentum import (
    generate_xt_momentum,
)



logger = logging.getLogger(__name__)


def get_gold_team_metrics(match_id: int):
    sql = """
        select
            match_id,
            team_name,
            goals,
            shots,
            xg,
            pass_completion_pct,
            progressive_passes,
            carries
        from serving.fact_gold_team
        where match_id = %(match_id)s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            return pd.read_sql(sql, conn, params={"match_id": match_id})

def get_gold_intervals_metrics(match_id: int):
    sql = """
        select
            match_id,
            team_name,
            interval_label,
            positive_xt
        from serving.fact_gold_intervals
        where match_id = %(match_id)s
    """
    with get_db_connection() as conn:
        return pd.read_sql(sql, conn, params={"match_id": match_id})

def get_silver_events(match_id: int) -> pd.DataFrame:
    sql = """
        select
            match_id,
            team_name,
            event_index,
            minute,
            second,
            start_x,
            start_y,
            shot_xg,
            outcome,
            is_shot
        from serving.stg_silver_events
        where match_id = %(match_id)s
    """
    with get_db_connection() as conn:
        return pd.read_sql(sql, conn, params={"match_id": match_id})


def generate_reports(match_id: int):
    gold_team_metrics = get_gold_team_metrics(match_id)
    gold_intervals_metrics = get_gold_intervals_metrics(match_id)
    silver_events = get_silver_events(match_id)
    outputs = [
        generate_match_summary(gold_team_metrics),
        generate_shot_map(silver_events),
        generate_xg_timeline(silver_events),
        generate_xt_momentum(gold_intervals_metrics),
    ]

    logger.info(
        "reports_generated",
        extra={
            "outputs": [str(path) for path in outputs],
            "report_count": len(outputs),
        },
    )

    return outputs


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    generate_reports(
        args.match_id
    )


if __name__ == "__main__":
    main()