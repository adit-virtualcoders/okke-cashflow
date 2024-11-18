with 

source as (
      select * from {{ source('web', 'receipt') }}
),

renamed as (
    select

        {{ adapter.quote("TenantId") }} as tenant_id,
        {{ adapter.quote("ReceiptId") }} as receipt_id,
        
        {{ adapter.quote("CreatedAt") }} as created_at,
        {{ adapter.quote("ModifiedAt") }} as modified_at

    from source
)

select * from renamed
