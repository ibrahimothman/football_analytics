select
    match_id,
    count(*) as team_count

from {{ ref('fact_gold_team') }}

group by match_id

having count(*) != 2

