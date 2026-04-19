select
    id,
    name,
    cast(ts as timestamp) as event_ts
from {{ ref('raw_events') }}
