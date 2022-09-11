{%- macro develop(develop_yml, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
    {{ return(adapter.dispatch('develop', 'dbt_metrics')(develop_yml, grain, dimensions, secondary_calculations, start_date, end_date, where)) }}
{%- endmacro -%}


{% macro default__develop(develop_yml, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
    {#- Need this here, since the actual ref is nested within loops/conditions: -#}
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}

    {% if not execute -%}
        {%- do return("not execute") -%}
    {%- endif %}
    
    {%- set develop_yml = fromyaml(develop_yml) -%}

    {# ############
    VALIDATION OF PROVIDED YML - Gotta make sure the metric looks good!
    ############ #}

    {%- if develop_yml.metrics | length > 1 -%}
        {%- do exceptions.raise_compiler_error("The develop macro only supports testing a single macro.") -%}
    {%- endif -%}

    {%- set metric_definition = develop_yml.metrics[0] -%}

    {%- if not metric_definition.name %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a name") -%}
    {%- endif %}

    {%- if not metric_definition.model %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a model") -%}
    {%- endif %}

    {%- if not metric_definition.timestamp %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a timestamp") -%}
    {%- endif %}

    {%- if not metric_definition.time_grains %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing time grains") -%}
    {%- endif %}

    {%- if grain not in metric_definition.time_grains %}
        {%- do exceptions.raise_compiler_error("The selected grain is missing from the metric definition yml") -%}
    {%- endif %}

    {%- if not metric_definition.calculation_method %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a metric calculation_method") -%}
    {%- endif %}

    {%- if metric_definition.calculation_method == 'derived' %}
        {%- do exceptions.raise_compiler_error("The develop macro does not support derived metrics") -%}
    {%- endif %}

    {%- if not metric_definition.expression %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing an expression field") -%}
    {%- endif %}

    {%- for dim in dimensions -%}
        {% if dim not in metric_definition.dimensions -%}
            {%- do exceptions.raise_compiler_error("The macro provided dimension is missing from the metric definition") %}
        {% endif %}
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

    {% set metric_list = [metric_definition.name] %}
    {% set metric_tree = dbt_metrics.get_faux_metric_tree(metric_list=metric_list) %}
    {% set metrics_dictionary = dbt_metrics.get_develop_metrics_dictionary(metric_tree=metric_tree, metric_definition=metric_definition) %}

    {# ############
    SECONDARY CALCULATION VALIDATION - Gotta make sure the secondary calcs are good!
    ############ #}

    {%- for calc_config in secondary_calculations if calc_config.aggregate %}
        {%- do dbt_metrics.validate_aggregate_coherence(metric_aggregate=metrics_dictionary[0].calculation_method, calculation_aggregate=calc_config.aggregate) %}
    {%- endfor %}

    {%- for calc_config in secondary_calculations if calc_config.period -%}
        {%- do dbt_metrics.validate_grain_order(metric_grain=grain, calculation_grain=calc_config.period) -%}
    {%- endfor -%}

    {# ############
    SQL GENERATION - Lets build that SQL!
    ############ -#}

    {%- set sql = dbt_metrics.get_metric_sql(
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
