select
    match_id,
    team_id
from {{ ref('fact_gold_team') }}
where goals > shots
    or completed_passes > attempted_passes
    or progressive_passes > completed_passes




