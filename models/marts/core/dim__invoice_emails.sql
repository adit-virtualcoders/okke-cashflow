with emails as (
    select
        customer_id,
        tenant_id,
        created_at,
        modified_at,
        case
            when json_value(log_entry, '$.IsReminder') = 'true' then cast(1 as bit)
            when json_value(log_entry, '$.IsReminder') = 'false' then cast(0 as bit)
        end as email_is_reminder,
            json_value(log_entry, '$.CustomerInvoiceId') as email_customer_invoice_id
    from
        {{ ref('stg_web__notification_log') }}
)

select distinct * from emails
