{% macro attribution_first_touch(session_at, conversion_at, user_id, conv_event) -%}
    
    case 
        when {{conversion_at}} is null then 0
        else (
            case
                when {{session_at}} = 
                    (first_value({{session_at}}) over (partition by {{user_id}}, {{conversion_at}}  order by {{session_at}})) 
                then 1

                when {{conversion_at}} is not null and {{session_at}} is null
                then 1

                else 0
            end
        )
    end as first_touch_{{conv_event}}

{%- endmacro %}
