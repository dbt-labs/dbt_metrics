{% macro validate_develop_metrics(metric_list, develop_yml) %}

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

        {%- if metric_definition.time_grains and grain %}
            {%- if grain not in metric_definition.time_grains %}
            {%- do exceptions.raise_compiler_error("The selected grain is missing from the metric definition of metric " ~ metric_definition.name ) -%}
            {%- endif %}
        {%- endif %}

        {%- if not metric_definition.expression %}
            {%- do exceptions.raise_compiler_error("The provided yml for metric " ~ metric_definition.name ~ " is missing an expression") -%}
        {%- endif %}

    {%- endfor -%}

{% endmacro %}