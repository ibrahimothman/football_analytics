select
    match_id,
    count(*) as number_of_rows
from {{ ref('fact_gold_intervals') }}
group by match_id
having count(*) != 40