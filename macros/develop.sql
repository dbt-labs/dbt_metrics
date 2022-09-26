{%- macro develop(develop_yml, metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
    {{ return(adapter.dispatch('develop', 'metrics')(develop_yml, metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where)) }}
{%- endmacro -%}


{% macro default__develop(develop_yml, metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
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

    {# {%- if develop_yml.metrics | length > 1 -%}
        {%- do exceptions.raise_compiler_error("The develop macro only supports testing a single macro.") -%}
    {%- endif -%} #}

    {% for metric_name in metric_list %}
        {% set metric_definition = develop_yml[metric_name] %}

        {%- if not metric_definition.name %}
            {%- do exceptions.raise_compiler_error("The provided yml is missing a metric name") -%}
        {%- endif %}

        {%- if not metric_definition.calculation_method %}
            {%- do exceptions.raise_compiler_error("The provided yml for metric " ~ metric_definition.name ~ " is missing a calculation method") -%}
        {%- endif %}

        {%- if not metric_definition.model and metric_definition.calculation_method != 'derived' %}
            {%- do exceptions.raise_compiler_error("The provided yml for metric " ~ metric_definition.name ~ " is missing a model") -%}
        {%- endif %}

        {%- if not metric_definition.timestamp %}
            {%- do exceptions.raise_compiler_error("The provided yml for metric " ~ metric_definition.name ~ " is missing a timestamp") -%}
        {%- endif %}

        {%- if not metric_definition.time_grains %}
            {%- do exceptions.raise_compiler_error("The provided yml for metric " ~ metric_definition.name ~ " is missing time grains") -%}
        {%- endif %}

        {%- if grain not in metric_definition.time_grains %}
            {%- do exceptions.raise_compiler_error("The selected grain is missing from the metric definition of metric " ~ metric_definition.name ) -%}
        {%- endif %}

        {%- if not metric_definition.expression %}
            {%- do exceptions.raise_compiler_error("The provided yml for metric " ~ metric_definition.name ~ " is missing an expression") -%}
        {%- endif %}

        {%- for dim in dimensions -%}
            {% if dim not in metric_definition.dimensions -%}
                {%- do exceptions.raise_compiler_error("The macro provided dimension is missing from the metric definition of metric " ~ metric_definition.name ) %}
            {% endif %}
        {%- endfor -%}


    {%- endfor -%}

    {# ############
    VALIDATION OF MACRO INPUTS - Making sure we have a provided grain!
    ############ #}

    {%- if not grain %}
        {%- do exceptions.raise_compiler_error("No date grain provided") %}
    {%- endif %}

    {# ############
    VARIABLE SETTING - Creating the faux metric tree and faux metric list. The faux fur of 2022
    ############ #}

    {% set metric_tree = metrics.get_faux_metric_tree(metric_list=metric_list, develop_yml=develop_yml) %}

    {% set metrics_dictionary = metrics.get_metrics_dictionary(metric_tree=metric_tree, develop_yml=develop_yml) %}

    {# ############
    SECONDARY CALCULATION VALIDATION - Gotta make sure the secondary calcs are good!
    ############ #}

    {%- do metrics.validate_develop_grain(grain=grain, metric_tree=metric_tree, metrics_dictionary=metrics_dictionary, secondary_calculations=secondary_calculations) -%}
    {%- do metrics.validate_metric_config(metrics_dictionary=metrics_dictionary) -%}

    {%- for calc_config in secondary_calculations if calc_config.aggregate %}
        {%- do metrics.validate_aggregate_coherence(metric_aggregate=metrics_dictionary[0].calculation_method, calculation_aggregate=calc_config.aggregate) %}
    {%- endfor %}

    {%- for calc_config in secondary_calculations if calc_config.period -%}
        {%- do metrics.validate_grain_order(metric_grain=grain, calculation_grain=calc_config.period) -%}
    {%- endfor -%}

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
        metric_tree=metric_tree
        ) %}
    ({{ sql }}) metric_subq

{%- endmacro %}
