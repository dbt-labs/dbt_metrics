{% macro calculate(metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) %}
    {{ return(adapter.dispatch('calculate', 'metrics')(metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where)) }}
{% endmacro %}


{% macro default__calculate(metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) %}
    {#- Need this here, since the actual ref is nested within loops/conditions: -#}
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}
    {# ############
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

    {%- if not grain -%}
        {%- do exceptions.raise_compiler_error("No date grain provided") -%}
    {%- endif -%}

    {%- if where is iterable and (where is not string and where is not mapping) -%}
        {%- do exceptions.raise_compiler_error("From v0.3.0 onwards, the where clause takes a single string, not a list of filters. Please fix to reflect this change") %}
    {%- endif -%}

    {%- do metrics.validate_grain(grain=grain, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, secondary_calculations=secondary_calculations) -%}

    {%- do metrics.validate_derived_metrics(metric_tree=metric_tree) -%}

    {%- do metrics.validate_dimension_list(dimensions=dimensions, metric_tree=metric_tree) -%} 

    {%- do metrics.validate_metric_config(metrics_dictionary=metrics_dictionary) -%} 

    {#- ############
    SECONDARY CALCULATION VALIDATION - Let there be window functions
    ############ -#}

    {%- for metric_name in metric_tree.base_set %}
        {%- for calc_config in secondary_calculations if calc_config.aggregate -%}
            {%- do metrics.validate_aggregate_coherence(metric_aggregate=metrics_dictionary[metric_name].calculation_method, calculation_aggregate=calc_config.aggregate) -%}
        {%- endfor -%}
    {%- endfor -%}

    {%- for calc_config in secondary_calculations if calc_config.period -%}
        {%- do metrics.validate_grain_order(metric_grain=grain, calculation_grain=calc_config.period) -%}
    {%- endfor -%} 

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
        metric_tree=metric_tree
    ) %}

({{ sql }}) metric_subq 

{%- endmacro -%}
