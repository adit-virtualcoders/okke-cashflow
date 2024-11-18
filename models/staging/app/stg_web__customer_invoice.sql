with source as (
      select * from {{ source('web', 'customer_invoice') }}
),
renamed as (
    select
        {{ adapter.quote("CustomerInvoiceId") }} as customer_invoice_id,
        {{ adapter.quote("CustomerId") }} as customer_id,
        {{ adapter.quote("TenantId") }} as tenant_id,

        {{ adapter.quote("InvoiceDate") }} as invoice_date, 
        
        {{ adapter.quote("DateEmailSent") }} as email_sent_at,
        {{ adapter.quote("DateReminderEmailSent") }} as reminder_email_sent_at,

        {{ adapter.quote("CreatedAt") }} as created_at,
        {{ adapter.quote("ModifiedAt") }} as modified_at

    from source
)
select * from renamed
  