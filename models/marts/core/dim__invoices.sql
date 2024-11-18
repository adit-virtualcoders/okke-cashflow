with customer_invoice as (

    select *
    from
        {{ ref('stg_web__customer_invoice') }}

),

invoice_emails as (

    select *
    from
        {{ ref('dim__invoice_emails') }}

),

invoices as (

    select
        ci.customer_invoice_id,
        ci.customer_id,
        ci.tenant_id,
        ci.invoice_date,
        coalesce(
            ci.email_sent_at,
            ies.created_at
        ) as email_sent_at,
        coalesce(
            ci.reminder_email_sent_at,
            iers.created_at
        ) as reminder_email_sent_at,
        ci.created_at,
        ci.modified_at
    from
        customer_invoice as ci
    left join
        invoice_emails as ies
        on
            ci.customer_id = ies.customer_id
            and ci.customer_invoice_id = ies.email_customer_invoice_id
            and ies.email_is_reminder = cast(0 as bit) -- Non-reminder emails
    left join
        invoice_emails as iers
        on
            ci.customer_id = iers.customer_id
            and ci.customer_invoice_id = iers.email_customer_invoice_id
            and iers.email_is_reminder = cast(1 as bit) -- Reminder emails

)

select * from invoices;
