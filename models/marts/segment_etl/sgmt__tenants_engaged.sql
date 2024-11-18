with fbclids as (
	select * from {{ ref('int__ad_fbclids') }}
),

gclids as (
    select * from {{ ref('int__ad_gclids')}}
),

tenant_users as (
    select * from {{ ref('dim__tenant_users')}}
),

segment_users as (
    select * from {{ ref('stg_segment_web__users') }}
),

engagements as (
    select * from {{ ref('int__engagements') }}
),

subscriptions as (
    select * from {{ ref('int__subscriptions_invoices') }}
),

tenants as (
    select
        e.tenant_id,
        coalesce(e.engaged_at,
            case when s.total_amount_paid > 0 then s.activated_at end
        ) as engaged_at
    from engagements e
    left join subscriptions s on e.chargebee_subscription_id = s.chargebee_subscription_id
),

tenants_enriched as (
    select
        tu.user_identifier as id,
        tu.user_identifier as user_id,
        t.*,
        -- Manually format the engaged_at date to avoid incorrect interpretation
        cast(year(t.engaged_at) as varchar(4)) + '-' +
        right('0' + cast(month(t.engaged_at) as varchar(2)), 2) + '-' +
        right('0' + cast(day(t.engaged_at) as varchar(2)), 2) + ' ' +
        right('0' + cast(datepart(hour, t.engaged_at) as varchar(2)), 2) + ':' +
        right('0' + cast(datepart(minute, t.engaged_at) as varchar(2)), 2) + ':' +
        right('0' + cast(datepart(second, t.engaged_at) as varchar(2)), 2) +
        case
            when datepart(tzoffset, t.engaged_at) >= 0 then '+'
            else '-'
        end +
        right('0' + cast(abs(datepart(tzoffset, t.engaged_at) / 60) as varchar(2)), 2) + ':' +
        right('0' + cast(abs(datepart(tzoffset, t.engaged_at) % 60) as varchar(2)), 2) as formatted_engaged_at,
        g.last_gclid,
        f.last_fbclid,

        coalesce(g.last_context_user_agent, f.last_context_user_agent) as last_context_user_agent,

        -- user traits
        su.context_ip as ip_address,
        su.email as email,
        case
            when su.phone is null then null
            when left(su.phone, 5) = '+6104' then
                '+61' + substring(su.phone, 5, len(su.phone) - 4)
            else su.phone
        end as phone
    from tenants t
    left join tenant_users tu on t.tenant_id = tu.tenant_id
    left join gclids g on tu.user_identifier = g.user_identifier
    left join fbclids f on tu.user_identifier = f.user_identifier
    left join segment_users su on tu.user_identifier = su.id
    where t.engaged_at >= '2024-01-01'
)

select * from tenants_enriched;
