with user_sessions as (
    select
        *
    from
        {{ ref('int__acquisition__user_sessions') }}
),

unpivoted_sessions as (
    select
        s.user_id,
        s.anonymous_id,
        {{ calculate_touchpoints('s.rfr_source') }},
        'Engaged' as attribution_status,
        s.engaged_at as attribution_date
    from user_sessions as s
    where s.engaged_at is not null
    group by s.user_id, s.anonymous_id, s.engaged_at

    union all

    select
        s.user_id,
        s.anonymous_id,
        {{ calculate_touchpoints('s.rfr_source') }},
        'Subscribed' as attribution_status,
        s.subscribed_at as attribution_date
    from user_sessions as s
    where s.subscribed_at is not null
    group by s.user_id, s.anonymous_id, s.subscribed_at

    union all

    select
        s.user_id,
        s.anonymous_id,
        {{ calculate_touchpoints('s.rfr_source') }},
        'Onboarded' as attribution_status,
        s.onboarded_at as attribution_date
    from user_sessions as s
    where s.onboarded_at is not null
    group by s.user_id, s.anonymous_id, s.onboarded_at

    union all

    select
        s.user_id,
        s.anonymous_id,
        {{ calculate_touchpoints('s.rfr_source') }},
        'Signed Up' as attribution_status,
        s.signed_up_at as attribution_date
    from user_sessions as s
    where s.signed_up_at is not null
    group by s.user_id, s.anonymous_id, s.signed_up_at
)

select * from unpivoted_sessions
