with source as (

    select * from {{ source('chargebee', 'invoices') }}

),

staged as (

    select 
        "list'][0]['invoice_id" as chargebee_invoice_id,
        "list'][0]['invoice_customer_id" as chargebee_customer_id,
        "list'][0]['invoice_subscription_id" as chargebee_subscription_id,

        status,
        cast(total as int) total,
        cast(amount_due as int) amount_due,
        cast(amount_paid as int) amount_paid,

        entity_description as line_item_1,
        item2_entity_description as line_item_2,

        dateadd(ss, cast("paid_at" as int), '19700101') as paid_at,
        dateadd(ss, cast("updated_at" as int), '19700101') as updated_at,
        dateadd(ss, cast("date" as int), '19700101') as customer_created_at,
        dateadd(ss, cast("due_date" as int), '19700101') as customer_updated_at
    
    from source

)

select * from staged