select
    count(*) as n_events,
    min(event_ts) as first_ts,
    max(event_ts) as last_ts
from {{ ref('mid_events') }}
