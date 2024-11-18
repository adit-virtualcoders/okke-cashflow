with source as (
      select * from {{ source('segment_web', 'identifies') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        
        {{ adapter.quote("user_id") }},
        {{ adapter.quote("anonymous_id") }},

        {{ adapter.quote("received_at") }},
        {{ adapter.quote("sent_at") }},
        {{ adapter.quote("timestamp") }} as event_at

    from source

)
select * from renamed
  