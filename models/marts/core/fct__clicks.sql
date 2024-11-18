with clicks as (

    select * from {{ ref('stg_google_ads__click_performance_reports') }}

),

campaigns as (

    select * from {{ ref('stg_google_ads__campaigns') }}

),

clicks_enriched as (

    select

        cl.gcl_id,
        cl.campaign_id,
        ca.name as campaign_name

    from clicks cl

    left join campaigns ca

    on cl.campaign_id = ca.id

)

select * from clicks_enriched
