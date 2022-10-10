{%- macro default__secondary_calculation_prior(metric_name, grain, dimensions, calc_config, metric_config) -%}
    
    {%- set calc_sql %}
            lag(
                {{ metric_name }}, {{ calc_config.interval }}
            ) over (
                {% if dimensions -%}
                    partition by {{ dimensions | join(", ") }} 
                {% endif -%}
                order by date_{{grain}}
            )
    {%- endset-%}
    
    {{ calc_sql }}

{% endmacro %}
