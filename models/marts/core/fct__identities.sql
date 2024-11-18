with 

staged as (

    select * from  {{ ref('stg_segment_web__identifies') }}

),

agged as (

    select
        
        user_id,
        anonymous_id,

        max(event_at) as event_at

    from staged

    group by user_id, anonymous_id

)

select * from agged
