select 
    match_id,
    team_id,
    period,
    interval_start
from {{ ref('fact_gold_intervals') }}
where interval_start % 5 != 0