{% macro develop(develop_yml, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None, allow_calendar_dimensions=False) -%}
    {{ return(adapter.dispatch('develop', 'metrics')(develop_yml, grain, dimensions, secondary_calculations, start_date, end_date, where, allow_calendar_dimensions)) }}
{% endmacro %}


{% macro default__develop(develop_yml, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None, allow_calendar_dimensions=False) -%}
    -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}

    {%- if not execute %}
        {%- do return("not execute") %}
    {%- endif %}
    
    {% set develop_yml = fromyaml(develop_yml)%}

    {# ############
    VALIDATION OF PROVIDED YML
    ############ #}
    
    {% if develop_yml["metrics"] | length > 1%}
        {%- do exceptions.raise_compiler_error("The develop macro only supports testing a single macro.") %}
    {% endif %}

    {% set metric_definition = develop_yml["metrics"][0] %}

    {%- if not metric_definition["name"] %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a name") %}
    {%- endif %}

    {%- if not metric_definition["model"] %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a model") %}
    {%- endif %}

    {%- if not metric_definition["timestamp"] %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a timestamp") %}
    {%- endif %}

    {%- if not metric_definition["time_grains"] %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing time grains") %}
    {%- endif %}

    {%- if grain not in metric_definition["time_grains"] %}
        {%- do exceptions.raise_compiler_error("The macro provided grain is missing from the metric definition yml") %}
    {%- endif %}

    {%- if not metric_definition["type"] %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a metric type") %}
    {%- endif %}

    {%- if metric_definition["type"] == 'expression' %}
        {%- do exceptions.raise_compiler_error("The develop macro does not support expression metrics") %}
    {%- endif %}

    {%- if not metric_definition["sql"] %}
        {%- do exceptions.raise_compiler_error("The provided yml is missing a sql field") %}
    {%- endif %}

    {% for dim in dimensions %}
        {% if dim not in metric_definition["dimensions"] %}
            {%- do exceptions.raise_compiler_error("The macro provided dimension is missing from the metric definition") %}
        {% endif %}
    {% endfor %}

    {# ############
    CREATING THE METRIC SQL
    ############ #}

    {%- set sql = metrics.get_metric_sql(
        metric_list=[],
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations,
        start_date=start_date,
        end_date=end_date,
        where=where,
        initiated_by='develop',
        metric_definition=metric_definition,
        allow_calendar_dimensions=allow_calendar_dimensions
    ) %}
    ({{ sql }}) metric_subq
{%- endmacro %}
