with

users as (

    select 
        *,
        created_at as event_at
    
    from {{ ref('dim__users') }}

    where created_at > {{ var("min_lead_date") }}

),

sessions_identified as (

    select * from {{ ref('int__sessions_identified') }}

),

sessions_conversions as (

    select
        -- Event
        coalesce(s.event_at, e.event_at) as session_at,
        e.event_at as attribution_at,

        e.user_id,
        e.user_identifier,

        'Signed Up' as event,
        e.name,

        -- Sessions
        s.event_id,
        s.anonymous_id,
        s.title,
        s.url,
        s.path,
        s.search,
        s.referrer,
        s.context_campaign_source,
        s.context_campaign_name,
        s.context_campaign_term,
        s.context_campaign_medium,
        s.rfr_medium,
        s.rfr_source,

        -- Channel
        coalesce(s.channel, 'unknown') as channel,

        -- Filters
        first_value(
            case when {{ is_non_direct('s.channel') }} then s.event_at end
        ) over (

            partition by e.event_at, e.user_identifier

            order by case when {{ is_non_direct('s.channel') }} then 0 else 1 end asc, s.event_at asc

        ) as first_non_direct,

        first_value(s.event_at) over (

            partition by e.event_at, e.user_identifier order by s.event_at asc

        ) as first_session

    from users e

    left join sessions_identified s 

    on e.user_identifier = s.user_identifier

        and (s.event_at between dateadd(dd, -{{ var("days_lookback_window") }}, e.event_at) and e.event_at)

),

sessions_filtered as (

    select * from sessions_conversions

    where 
        {{ is_non_direct('channel') }}

        or (
            first_non_direct is null and session_at = first_session
        )

),

sessions_attributed as (

    select *,

        {{ attribution_cols('session_at', 'attribution_at', 'user_identifier', 'acquisition') }}

    from sessions_filtered

)

select * from sessions_attributed
