{{
    config(
        materialized='incremental',
        unique_key="event_id",
    )
}}

with 

pages as (

    select * from {{ ref('fct__pages') }}

),

clicks as (

    select * from {{ ref('fct__clicks') }}

),

pages_referrer as (

    select
        p.*,

        r.medium as rfr_medium,
        r.source as rfr_source,
        row_number() over (partition by p.event_id order by r.source) as rn

    from pages p

    left join {{ default_schema }}.dim__referrers r
    
    on {{ clean_referrer('p.referrer') }} = r.url

),

pages_deduped as (

    select *

    from pages_referrer

    where rn = 1

),

pages_enriched as (

    select 
        *,

        coalesce(
            {{ get_query_param('search', 'gclid') }},
            {{ get_query_param('search', 'wbraid') }},
            {{ get_query_param('search', 'gbraid') }} 
        )  as gclid,

        {{ is_paid_from_search('search') }} as is_paid,

        {{ is_branded_from_url('path', 'search') }} as is_branded

    from pages_deduped

),

pages_campaigns as (

    select
        p.*,
        c.campaign_name click_campaign_name

    from pages_enriched p

    left join clicks c

    on p.gclid = c.gcl_id

),

pages_channels as (

    select
        *,

        case

            when (
                coalesce(context_campaign_medium, rfr_medium) = 'social' 
                or (context_campaign_source like '%facebook%')
                or referrer like '%linkedin%'
                or search like '%li_fat_id%'
                or search like '%fbclid%'
            )
            then concat(is_paid, 'social')

            when (
                (rfr_medium = 'search') 
                or (context_campaign_source in ('bing', 'google', 'google_mybusiness'))
                or (gclid is not null) -- Google
                or referrer like '%utm_medium=cpc%'
            ) 
            then concat(is_branded, is_paid, 'search')

            when (
                context_campaign_medium = 'email'
                or context_campaign_source in ('customer.io', 'vero')
            ) then 'email'

            when rfr_medium = 'paid' then 'display'

            when (referrer like '%okokke.com%') then 'internal'

            when referrer is not null then 'referral'

            else 'direct / none'
        
        end as channel

    from pages_campaigns

)

select * from pages_channels
