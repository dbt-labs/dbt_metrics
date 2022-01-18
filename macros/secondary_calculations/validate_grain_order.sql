{% macro validate_grain_order(metric_grain, calculation_grain) %}
    -- TODO: Can you have the _same_ grain? I think so, it doesn't make sense but it's not broken

    {% set grains = metrics.get_grain_order() %}
    {% set metric_grain_index = grains.index(metric_grain) %}
    {% set calculation_grain_index = grains.index(calculation_grain) %}

    {% if (calculation_grain_index < metric_grain_index) %}
        {% do exceptions.raise_compiler_error("Can't calculate secondary metric at " ~ calculation_grain ~"-level when metric is at " ~ metric_grain ~ "-level") %}
    {% endif %}
{% endmacro %}

{% macro get_grain_order() %}
    {{ return(adapter.dispatch('get_grain_order', 'metrics')()) }}
{% endmacro %}

{% macro default__get_grain_order() %}
    {% do return (['day', 'week', 'month', 'quarter', 'year']) %}
{% endmacro %}