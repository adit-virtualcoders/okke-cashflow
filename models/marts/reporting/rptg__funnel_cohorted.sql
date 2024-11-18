with 

pages as (

    select * from {{ ref('fct__pages') }}

),

users as (

    select * from {{ ref('dim__users') }}

),

subscriptions as (

    select * from {{ ref('int__subscriptions_invoices') }}

),

tenants as (

    select * from {{ ref('dim__tenants') }}

),

tenant_users as (

    select * from {{ ref('dim__tenant_users') }}

),

engagements as (

    select * from {{ ref('int__engagements') }}

),

cohorted as (

    select

        u.user_id,
        -- t.tenant_id,

        -- Timestamps
        cast(min(u.created_at) as date)                     as signed_up_at,
        NULL                                                as email_delivered_at,
        NULL                                                as verified_at,
        cast(min(t.created_at) as date)                     as onboarded_at, -- First tenant created
        cast(min(e.engaged_at) as date)                     as engaged_at, -- First tenant activated
        cast(min((s.activated_at
            at time zone 'AUS Central Standard Time')
            at time zone 'UTC') as date
        )                                                   as subscribed_at,
        cast(min(case
                    when s.activated_at is not null         -- Only look at cancellations of subscribers
                        then (s.cancelled_at
                        at time zone 'AUS Central Standard Time')
                        at time zone 'UTC'
            end) as date)                                     as cancelled_at,

        cast(min(e.first_invoice_sent_at) as date)          as invoice_sent_7days_at,
        cast(min(e.first_expense_created_at) as date)       as expense_logged_7days_at,

        -- Time Between Stages
        datediff(second, u.created_at, min(t.created_at))   as onboarded_after_seconds,
        datediff(second, u.created_at, min(e.engaged_at))   as engaged_after_seconds,
        datediff(second, u.created_at, min((s.activated_at
            at time zone 'AUS Central Standard Time')
            at time zone 'UTC'))                            as subscribed_after_seconds,

        -- Counts
        count(t.tenant_id)                                  as accounts

    from users u
         
    left join tenant_users tu
        
        on u.user_id = tu.user_id
    
    left join tenants t
        
        on tu.tenant_id = t.tenant_id
    
    left join engagements e

        on t.tenant_id = e.tenant_id
    
    left join subscriptions s
        
        on t.chargebee_subscription_id = s.chargebee_subscription_id

    where 
        
        u.created_at > '2022-09-20'                               -- Exclude accounts with weird behaviours
        
        and (u.created_at < t.created_at or t.created_at is null)   -- Exclude users added to existing accounts

    group by u.user_id, u.created_at

),

enriched as (

    select 
        *,

        case
            when cancelled_at is not null then 'Cancelled'
            when subscribed_at is not null then 'Subscribed'
            when engaged_at is not null then 'Engaged'
            when onboarded_at is not null then 'Tenant Created'
            when signed_up_at is not null then 'Signed Up'
        end as current_stage

    from cohorted

)

select * from enriched
