select
    match_id,
    team_id,
    period,
    interval_start,
    is_stoppage_time,
    interval_label
from {{ ref('fact_gold_intervals') }}
where abs(positive_xt + negative_xt - net_xt) > 1e-10