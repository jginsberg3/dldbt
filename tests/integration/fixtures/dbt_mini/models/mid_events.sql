select
    id,
    upper(name) as name_upper,
    event_ts
from {{ ref('stg_events') }}
