{% macro attribution_time_decay(session_at, conversion_at, user_id, conv_event) -%}

    case 
        when {{conversion_at}} is null then 0
        else (
            case
                when (
                        count({{conversion_at}}) over (partition by {{user_id}}, {{conversion_at}} )
                    ) = 1 then 1

                when (
                        count({{conversion_at}}) over (partition by {{user_id}}, {{conversion_at}} )
                    ) = (
                        row_number() over (partition by {{user_id}}, {{conversion_at}}  order by {{session_at}} desc)
                    ) then (
                        1.0 / pow(2.0, (
                            row_number() over (partition by {{user_id}}, {{conversion_at}}  order by {{session_at}} desc)
                        ) - 1 )
                    )

                else (
                    1.0 / pow(2.0, 
                        row_number() over (partition by {{user_id}}, {{conversion_at}}  order by {{session_at}} desc)
                    )
                )
            end
        )
    end as time_decay_{{conv_event}}

{%- endmacro %}
