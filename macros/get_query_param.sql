{% macro get_query_param(search, param) -%}

    case 
        when {{ search }} like '%{{ param }}=%'
        then 
            substring(
                {{ search }}, 
                
                charindex('{{ param }}' + '=', {{ search }}) + len('{{ param }}='), 

                charindex('&', {{ search }} + '&', charindex('{{ param }}' + '=', {{ search }}) + len('{{ param }}=') + 1)
                 	- (charindex('{{ param }}' + '=', {{ search }}) + len('{{ param }}='))
            )

    end

{%- endmacro %}