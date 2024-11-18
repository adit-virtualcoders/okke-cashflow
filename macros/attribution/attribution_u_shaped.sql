{% macro attribution_u_shaped(session_at, conversion_at, user_id, conv_event) -%}

    case 
        when {{conversion_at}} is null then 0
        else (
            case
                when (
                        count({{conversion_at}}) over (partition by {{user_id}}, {{conversion_at}} )
                    ) = 1 then 1

                when (
                        count({{conversion_at}}) over (partition by {{user_id}}, {{conversion_at}} )
                    ) = 2 then 0.5

                when {{session_at}} = 
                    (first_value({{session_at}}) over (partition by {{user_id}}, {{conversion_at}}  order by {{session_at}})) 
                then 0.4

                when {{session_at}} = 
                    (first_value({{session_at}}) over (partition by {{user_id}}, {{conversion_at}}  order by {{session_at}} desc)) 
                then 0.4

                else 0.2 / ((count({{conversion_at}}) over (partition by {{user_id}}, {{conversion_at}} )) - 2)
            end
        )
    end as u_shaped_{{conv_event}}

{%- endmacro %}
