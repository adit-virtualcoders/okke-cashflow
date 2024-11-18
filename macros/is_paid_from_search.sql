{% macro is_paid_from_search(search) -%}
    case

        when {{ search }} like '%gclid%' 
            or {{ search }} like '%fbclid%'
            or {{ search }} like '%li_fat_id%'
            or {{ search }} like '%msclkid%' 
            or {{ search }} like '%utm_medium=cpc%'
            or {{ search }} like '%utm_medium=paid%'
        then 'paid '
        
        else 'organic '

    end
{%- endmacro %}