with source as (
      select * from {{ source('google_ads', 'click_performance_reports') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("received_at") }},
        {{ adapter.quote("slot") }},
        {{ adapter.quote("uuid_ts") }},
        {{ adapter.quote("ad_group_id") }},
        {{ adapter.quote("adwords_customer_id") }},
        {{ adapter.quote("aoi_most_specific_target_id") }},
        {{ adapter.quote("date_start") }},
        {{ adapter.quote("date_stop") }},
        {{ adapter.quote("page") }},
        {{ adapter.quote("click_type") }},
        {{ adapter.quote("device") }},
        {{ adapter.quote("gcl_id") }},
        {{ adapter.quote("user_list_id") }},
        {{ adapter.quote("ad_network_type") }},
        {{ adapter.quote("campaign_id") }},
        {{ adapter.quote("creative_id") }}

    from source
)
select * from renamed
  