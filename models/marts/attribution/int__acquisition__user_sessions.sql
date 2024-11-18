with
users as (
    select
        user_id,
        user_identifier
    from
        {{ ref('dim__users') }}
),

sessions as (
    select
        user_id,
        user_identifier,
        anonymous_id,
        user_session_id,
        rfr_source
    from
        {{ ref('int__sessions_identified') }}
),

calculated_users as (
    select
        u.user_id,
        cast(min(u.created_at) as date) as signed_up_at,
        cast(min(t.created_at) as date) as onboarded_at,
        cast(min(e.engaged_at) as date) as engaged_at,
        cast(min(
            (s.activated_at at time zone 'AUS Central Standard Time') at time zone 'UTC'
        ) as date) as subscribed_at
    from
        {{ ref('dim__users') }} as u
    left join {{ ref('dim__tenant_users') }} as tu
        on u.user_id = tu.user_id
    left join {{ ref('dim__tenants') }} as t
        on tu.tenant_id = t.tenant_id
    left join {{ ref('int__engagements') }} as e
        on t.tenant_id = e.tenant_id
    left join {{ ref('int__subscriptions_invoices') }} as s
        on t.chargebee_subscription_id = s.chargebee_subscription_id
    where
        u.created_at > '2022-09-20'
        and (u.created_at < t.created_at or t.created_at is null)
    group by
        u.user_id
),

user_sessions as (
    select
        u.user_id,
        s.anonymous_id,
        s.user_session_id,
        s.rfr_source,
        cu.engaged_at,
        cu.onboarded_at,
        cu.subscribed_at,
        cu.signed_up_at
    from
        users as u
    inner join sessions as s
            on u.user_identifier = s.user_identifier
            or u.user_identifier = s.anonymous_id
            or u.user_identifier = s.user_id
    inner join calculated_users as cu
        on u.user_id = cu.user_id
    where
        s.rfr_source in ('Google', 'Facebook', 'Instagram', 'Gmail', 'Youtube')
)

select
    *
from
    user_sessions
