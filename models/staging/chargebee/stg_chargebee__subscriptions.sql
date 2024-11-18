with source as (

    select * from {{ source('chargebee', 'subscriptions') }}

),

staged as (

    select 
        "list'][0]['subscription_id" as chargebee_subscription_id,
        "list'][0]['customer_id" as chargebee_customer_id,

        "list'][0]['subscription_status" as subscription_status,
        dateadd(ss, cast("list'][0]['subscription_created_at" as int), '19700101') as subscription_created_at,
        dateadd(ss, cast("list'][0]['subscription_updated_at" as int), '19700101') as subscription_updated_at,

        mrr,
        quantity,
        unit_price,
        item_price_id,
        item_type,
        amount,
    
        dateadd(ss, cast(trial_start as int), '19700101') as trial_start,
        dateadd(ss, cast(trial_end as int), '19700101') as trial_end,
        dateadd(ss, cast(current_term_start as int), '19700101') as current_term_start,
        dateadd(ss, cast(current_term_end as int), '19700101') as current_term_end,
        dateadd(ss, cast(next_billing_at as int), '19700101') as next_billing_at,

        dateadd(ss, cast(started_at as int), '19700101') as started_at,
        dateadd(ss, cast(activated_at as int), '19700101') as activated_at,
        dateadd(ss, cast(cancelled_at as int), '19700101') as cancelled_at,
        dateadd(ss, cast(cancel_schedule_created_at as int), '19700101') as cancel_schedule_created_at,

        billing_period,
        billing_period_unit,

        promotional_credits,
        refundable_credits,
        due_invoices_count,

        -- Customers
        email,
        phone,
        company,
        "list'][0]['customer_first_name" as customer_first_name,
        "list'][0]['customer_last_name" as customer_last_name,
        
        dateadd(ss, cast("list'][0]['customer_created_at" as int), '19700101') as customer_created_at,
        dateadd(ss, cast("list'][0]['customer_updated_at" as int), '19700101') as customer_updated_at        
    
    from source

)

select * from staged