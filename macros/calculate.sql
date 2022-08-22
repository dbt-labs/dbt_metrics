{% macro calculate(metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
    {{ return(adapter.dispatch('calculate', 'metrics')(metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where)) }}
{% endmacro %}


{% macro default__calculate(metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
    -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}

    {# ############
    VARIABLE SETTING - Creating the metric tree and making sure metric list is a list!
    ############ #}

    {% if metric_list is not iterable %}
        {% set metric_list = [metric_list] %}
    {% endif %}

    {% set metric_tree = metrics.get_metric_tree(metric_list) %}

    {# ############
    SQL GEN VARIABLE SETTING - Gotta catch all those variables! Common dimension list is the pikachu
    ############ #}

    {# We have to break out calendar dimensions as their own list of acceptable dimensions. 
    This is because of the date-spining. If we don't do this, it creates impossible combinations
    of calendar dimension + base dimensions #}
    {%- set calendar_dimensions = metrics.get_calendar_dimension_list() -%}

    {# Additionally, we also have to restrict the dimensions coming in from the macro to 
    no longer include those we've designated as calendar dimensions. That way they 
    are correctly handled by the spining. We override the dimensions variable for 
    cleanliness #}
    {%- set non_calendar_dimensions = metrics.get_non_calendar_dimension_list(dimensions, calendar_dimensions) -%}

    {# Finally we set the relevant periods, which is a list of all time grains that need to be contained
    within the final dataset in order to accomplish base + secondary calc functionality. #}
    {%- set relevant_periods = metrics.get_relevent_periods(grain, secondary_calculations) %}

    {# ############
    VALIDATION - Make sure everything is good!
    ############ #}

    {%- if not execute %}
        {%- do return("Did not execute") %}
    {%- endif %}

    {%- if not metric_list %}
        {%- do exceptions.raise_compiler_error("No metric or metrics provided") %}
    {%- endif %}

    {%- if not grain %}
        {%- do exceptions.raise_compiler_error("No date grain provided") %}
    {%- endif %}

    {% if where is iterable and (where is not string and where is not mapping) %}
        {%- do exceptions.raise_compiler_error("From v0.3.0 onwards, the where clause takes a single string, not a list of filters. Please fix to reflect this change") %}
    {% endif %}

    {% do metrics.validate_grain(grain, metric_tree['full_set'], metric_tree['base_set'])%}

    {% do metrics.validate_expression_metrics(metric_tree['full_set'])%}

    {% do metrics.validate_dimension_list(dimensions, metric_tree['full_set'], calendar_dimensions) %}

    {# ############
    SECONDARY CALCULATION VALIDATION - Let there be window functions
    ############ #}

    {% for metric in metric_list %}
        {% set metric_type = metric.type%}
        {%- for calc_config in secondary_calculations if calc_config.aggregate %}
            {%- do metrics.validate_aggregate_coherence(metric_type, calc_config.aggregate) %}
        {%- endfor %}
    {%endfor%}

    {%- for calc_config in secondary_calculations if calc_config.period %}
        {%- do metrics.validate_grain_order(grain, calc_config.period) %}
    {%- endfor %}

    {# ############
    SQL GENERATION - Lets build that SQL!
    ############ #}

    {%- set sql = metrics.get_metric_sql(
        metric_list=metric_list,
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations,
        start_date=start_date,
        end_date=end_date,
        where=where,
        initiated_by='calculate',
        metric_tree=metric_tree,
        calendar_dimensions=calendar_dimensions,
        non_calendar_dimensions=non_calendar_dimensions,
        relevant_periods=relevant_periods
    ) %}
    ({{ sql }}) metric_subq
{%- endmacro %}
