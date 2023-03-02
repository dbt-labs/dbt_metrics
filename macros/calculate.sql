{% macro calculate(metric_list, grain=none, dimensions=[], secondary_calculations=[], start_date=none, end_date=none, where=none, date_alias=none) %}
    {{ return(adapter.dispatch('calculate', 'metrics')(metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where, date_alias)) }}
{% endmacro %}


{% macro default__calculate(metric_list, grain=none, dimensions=[], secondary_calculations=[], start_date=none, end_date=none, where=none, date_alias=none) %}
    {#- Need this here, since the actual ref is nested within loops/conditions: -#}
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}
    
    {#- ############
    VARIABLE SETTING - Creating the metric tree and making sure metric list is a list!
    ############ -#}

    {%- if metric_list is not iterable -%}
        {%- set metric_list = [metric_list] -%}
    {%- endif -%}

    {%- set metric_tree = metrics.get_metric_tree(metric_list=metric_list) -%}

    {#- Here we are creating the metrics dictionary which contains all of the metric information needed for sql gen. -#}
    {%- set metrics_dictionary = metrics.get_metrics_dictionary(metric_tree=metric_tree) -%}

    {#- ############
    VALIDATION - Make sure everything is good!
    ############ -#}

    {%- if not execute -%}
        {%- do return("Did not execute") -%}
    {%- endif -%}

    {%- if not metric_list -%}
        {%- do exceptions.raise_compiler_error("No metric or metrics provided") -%}
    {%- endif -%}

    {%- do metrics.validate_timestamp(grain=grain, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, dimensions=dimensions) -%}

    {%- do metrics.validate_grain(grain=grain, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, secondary_calculations=secondary_calculations) -%}

    {%- do metrics.validate_derived_metrics(metric_tree=metric_tree) -%}

    {%- do metrics.validate_dimension_list(dimensions=dimensions, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary) -%} 

    {%- do metrics.validate_metric_config(metrics_dictionary=metrics_dictionary) -%} 

    {%- do metrics.validate_where(where=where) -%} 

    {%- do metrics.validate_secondary_calculations(metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, grain=grain, secondary_calculations=secondary_calculations) -%} 

    {%- do metrics.validate_calendar_model() -%}

    {#- ############
    SQL GENERATION - Lets build that SQL!
    ############ -#}

    {%- set sql = metrics.get_metric_sql(
        metrics_dictionary=metrics_dictionary,
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations,
        start_date=start_date,
        end_date=end_date,
        where=where,
        date_alias=date_alias,
        metric_tree=metric_tree
    ) %}

({{ sql }}) metric_subq 

{%- endmacro -%}
