with source as (
      select * from {{ source('google_ads', 'campaigns') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("received_at") }},
        {{ adapter.quote("adwords_customer_id") }},
        {{ adapter.quote("end_date") }},
        {{ adapter.quote("start_date") }},
        {{ adapter.quote("name") }},
        {{ adapter.quote("serving_status") }},
        {{ adapter.quote("status") }},
        {{ adapter.quote("uuid_ts") }}

    from source
)
select * from renamed
