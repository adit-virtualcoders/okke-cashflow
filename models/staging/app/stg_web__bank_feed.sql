with source as (
      select * from {{ source('web', 'bank_feed') }}
),
renamed as (
    select
        {{ adapter.quote("BankFeedUserId") }} as bank_feed_id,
        {{ adapter.quote("TenantId") }} as tenant_id,

        {{ adapter.quote("CreatedAt") }} as created_at,
        {{ adapter.quote("ModifiedAt") }} as modified_at

    from source
)
select * from renamed
  