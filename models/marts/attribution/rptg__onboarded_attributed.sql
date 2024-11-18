with

tenant_users as (

    select
        *,
        tenant_created_at as event_at

    from {{ ref('dim__tenant_users') }}

    where tenant_created_at > {{ var("min_lead_date") }}

),

chargebee_subscriptions as (

    select * from {{ ref('dim__chargebee_subscriptions') }}

),

invoices as (

    select * from {{ ref('int__subscriptions_invoices')}}

),

engaged as (

    select * from {{ ref('int__engagements') }}

),

tenants_filtered as (

    select tu.*, cs.chargebee_customer_id from tenant_users tu

    left join chargebee_subscriptions cs

    on tu.chargebee_subscription_id = cs.chargebee_subscription_id

    where {{ dedupe_filter('cs.email') }}

),

sessions_identified as (

    select * from {{ ref('int__sessions_identified') }}

),

sessions_conversions as (

    select
        -- Event
        coalesce(s.event_at, e.event_at) as session_at,
        e.event_at as attribution_at,
        datetrunc(day, e.event_at) as attribution_date,

        e.tenant_id,
        e.chargebee_subscription_id,
        e.chargebee_customer_id,
        e.user_id,
        e.user_identifier,

        'Tenant Created' as event,
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
        coalesce(s.click_campaign_name, s.context_campaign_name) as context_campaign_name,
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

    from tenants_filtered e

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

        {{ attribution_cols('session_at', 'attribution_at', 'tenant_id', 'onboarded') }}

    from sessions_filtered

),

enriched as (

    select s.*,
        i.total_amount_paid as clv,
        case
            when i.total_amount_paid > 0 then 'Subscribed'
            when e.engaged_at is not null then 'Engaged'
            else 'Onboarded'
        end as subscription_status
    
    from sessions_attributed s

    left join invoices i

    on s.chargebee_subscription_id = i.chargebee_subscription_id

        and s.first_touch_onboarded = 1

    left join engaged e

    on s.tenant_id = e.tenant_id

)

select * from enriched
