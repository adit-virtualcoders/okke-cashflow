with source as (
      select * from {{ source('segment_web', 'users') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        
        {{ adapter.quote("full_name") }},
        
        {{ adapter.quote("uuid_ts") }},
        
        {{ adapter.quote("onboarded") }},
        
        {{ adapter.quote("_id") }},
        
        {{ adapter.quote("email") }},
        {{ adapter.quote("phone") }},
        
        {{ adapter.quote("created_at") }},
        {{ adapter.quote("companies") }},

        {{ adapter.quote("context_ip") }},

        {{ adapter.quote("billing_address_city") }},
        {{ adapter.quote("billing_address_country") }},
        {{ adapter.quote("billing_address_postcode") }},
        {{ adapter.quote("billing_address_state") }},
        {{ adapter.quote("billing_business_name") }},
        {{ adapter.quote("billing_address_line1") }},
        {{ adapter.quote("billing_address_line2") }},
        {{ adapter.quote("billing_address_postal_code") }},

        {{ adapter.quote("active_company_created_at") }},
        {{ adapter.quote("active_company_address_city") }},
        {{ adapter.quote("active_company_plan_trial_end_at") }},
        {{ adapter.quote("active_company_website") }},
        {{ adapter.quote("active_company_name") }},
        {{ adapter.quote("active_company_role") }},
        {{ adapter.quote("active_company_address_country") }},
        {{ adapter.quote("active_company_id") }},
        {{ adapter.quote("active_company_business_number_number") }},
        {{ adapter.quote("active_company_business_number_type") }},
        {{ adapter.quote("active_company_plan_type") }},
        {{ adapter.quote("active_company_address_line1") }},
        {{ adapter.quote("active_company_address_postal_code") }},
        {{ adapter.quote("active_company_address_state") }},
        {{ adapter.quote("active_company_address_line2") }},

        {{ adapter.quote("received_at") }}

    from source
)
select * from renamed
  