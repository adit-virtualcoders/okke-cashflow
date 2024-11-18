with source as (
      select * from {{ source('prod', 'users') }}
),
renamed as (
    select
        {{ adapter.quote("UserId") }} as user_id,
        {{ adapter.quote("UserIdentifier") }} as user_identifier,

        {{ adapter.quote("Name") }} as name,
        
        convert(datetime, {{ adapter.quote("CreatedAt") }}, 1) as created_at,
        convert(datetime, {{ adapter.quote("ModifiedAt") }}, 1) as modified_at

    from source

    where "UserId" != 4317
)
select * from renamed
