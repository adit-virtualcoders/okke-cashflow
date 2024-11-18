{% macro is_branded_from_url(path, search) -%}
    case
        when 
            {{ search }} like '%search_nonbrand_generic%'
            then 'unbranded '

        when 
            {{ path }} = '/'

            or {{ search }} like '%utm_campaign=brand%'

        then 'branded '

        else 'unbranded '

    end
{%- endmacro %}