{%- macro develop(develop_yml, metric_list, grain=none, dimensions=[], secondary_calculations=[], start_date=none, end_date=none, where=none, date_alias=none) -%}
    {{ return(adapter.dispatch('develop', 'metrics')(develop_yml, metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where, date_alias)) }}
{%- endmacro -%}


{% macro default__develop(develop_yml, metric_list, grain=none, dimensions=[], secondary_calculations=[], start_date=none, end_date=none, where=none, date_alias=none) -%}
    {#- Need this here, since the actual ref is nested within loops/conditions: -#}
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}

    {% if not execute -%}
        {%- do return("not execute") -%}
    {%- endif %}

    {%- if metric_list is string -%}
        {%- set metric_list = [metric_list] -%}
    {%- endif -%}

    {# For the sake of consistency with metrics definition and the ability to easily
    reference the metric object, we are creating a metrics_dictionary for set of metrics
    included in the provided yml. This is used to construct the metric tree #}
    {%- set develop_yml = fromyaml(develop_yml) -%}

    {% set develop_dictionary = {} %}
    {% for metric_definition in develop_yml.metrics %}
        {% do develop_dictionary.update({metric_definition.name:{}}) %}
        {% do develop_dictionary.update({metric_definition.name:metric_definition}) %}
    {% endfor %}
    {% set develop_yml = develop_dictionary %}

    {# ############
    VALIDATION OF PROVIDED YML - Gotta make sure the metric looks good!
    ############ #}

    {%- do metrics.validate_develop_metrics(metric_list=metric_list, develop_yml=develop_yml) -%}

    {# ############
    VARIABLE SETTING - Creating the faux metric tree and faux metric list. The faux fur of 2022
    ############ #}

    {% set metric_tree = metrics.get_faux_metric_tree(metric_list=metric_list, develop_yml=develop_yml) %}

    {% set metrics_dictionary = metrics.get_metrics_dictionary(metric_tree=metric_tree, develop_yml=develop_yml) %}

    {# ############
    SECONDARY VALIDATION - Gotta make sure everything else is good!
    ############ #}

    {%- do metrics.validate_timestamp(grain=grain, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, dimensions=dimensions) -%}

    {%- do metrics.validate_grain(grain=grain, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, secondary_calculations=secondary_calculations) -%}
    
    {%- do metrics.validate_dimension_list(dimensions=dimensions, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary) -%} 

    {%- do metrics.validate_metric_config(metrics_dictionary=metrics_dictionary) -%}

    {%- do metrics.validate_secondary_calculations(metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, grain=grain, secondary_calculations=secondary_calculations) -%} 

    {%- do metrics.validate_where(where=where) -%} 

    {%- do metrics.validate_calendar_model() -%}


    {# ############
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

{%- endmacro %}
