{% macro attribution_linear(session_at, conversion_at, user_id, conv_event) -%}

    case 
        when {{conversion_at}} is null then 0

        else (
            round((1 * 1E0) / 
                (count({{conversion_at}}) over (partition by {{user_id}}, {{conversion_at}} )), 3)
        )

    end as linear_{{ conv_event }}

{%- endmacro %}
