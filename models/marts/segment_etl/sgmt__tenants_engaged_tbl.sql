{{
    config(
        materialized='table'
    )    
}}

with

engaged as (

    select *,

        row_number() over (partition by id order by engaged_at desc) as rn
    
    from {{ ref('sgmt__tenants_engaged') }}

)

select * from engaged where rn = 1
