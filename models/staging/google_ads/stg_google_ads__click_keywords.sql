with source as (
      select * from {{ source('google_ads__ab', 'google_ads__click_view') }}
),

renamed as (
    select * from source
)

select * from renamed
