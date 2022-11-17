{%- macro gen_filters(metric_dictionary, start_date, end_date) -%}
    {{ return(adapter.dispatch('gen_filters', 'metrics')(metric_dictionary, start_date, end_date)) }}
{%- endmacro -%}

{%- macro default__gen_filters(metric_dictionary, start_date, end_date) -%}

    {#- metric start/end dates also applied here to limit incoming data -#}
    {% if start_date or end_date %}
        and (
        {% if start_date and end_date -%}
            cast(base_model.{{metric_dictionary.timestamp}} as date) >= cast('{{ start_date }}' as date)
            and cast(base_model.{{metric_dictionary.timestamp}} as date) <= cast('{{ end_date }}' as date)
        {%- elif start_date and not end_date -%}
            cast(base_model.{{metric_dictionary.timestamp}} as date) >= cast('{{ start_date }}' as date)
        {%- elif end_date and not start_date -%}
            cast(base_model.{{metric_dictionary.timestamp}} as date) <= cast('{{ end_date }}' as date)
        {%- endif %} 
        )
    {% endif -%} 

    {#- metric filter clauses... -#}
    {% if metric_dictionary.filters %}
        and (
            {% for filter in metric_dictionary.filters -%}
                {%- if not loop.first -%} and {% endif %}{{ filter.field }} {{ filter.operator }} {{ filter.value }}
            {% endfor -%}
        )
    {% endif -%}

{%- endmacro -%}