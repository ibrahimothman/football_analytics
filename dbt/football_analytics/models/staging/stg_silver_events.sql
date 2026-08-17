{{ config(
    materialized='incremental',
    unique_key='match_id',
) }}
select 
    *
from {{ source('football_analytics', 'src_silver_events') }}

{% if is_incremental() %}
where match_id = {{ var('match_id') }}
{% endif %}