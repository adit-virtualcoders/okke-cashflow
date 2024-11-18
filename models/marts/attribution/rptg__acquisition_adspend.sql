with unpivoted_sessions as (
    select
        *
    from
        {{ ref('int__acquisition__unpivoted_sessions')}}
),

final_result as (
    select
        ts.attribution_date,
        ts.attribution_status,
        sum(ts.google_touchpoints) as gt_touchpoints,
        sum(ts.meta_touchpoints) as mt_touchpoints
    from unpivoted_sessions as ts
    group by ts.attribution_date, ts.attribution_status
)

select *
from final_result;
