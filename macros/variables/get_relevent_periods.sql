{%- macro get_relevent_periods(grain, secondary_calculations) %}

    {%- set relevant_periods = [] %}
    {%- for calc_config in secondary_calculations if calc_config.period and calc_config.period not in relevant_periods and calc_config.period != grain %}
        {%- do relevant_periods.append(calc_config.period) %}
    {%- endfor -%}

    {%- do return(relevant_periods)-%}

{% endmacro %}