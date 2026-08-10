"""Generate match reports from Silver and Gold data."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

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

def generate_reports(
    match_id: int,
    silver_path: Path,
    gold_path: Path,
    gold_intervals_path: Path,
) -> list[Path]:

    outputs = [
        generate_match_summary(
            gold_path
        ),

        generate_shot_map(
            silver_path
        ),

        generate_xg_timeline(
            silver_path
        ),

        generate_xt_momentum(
            gold_intervals_path
        ),
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