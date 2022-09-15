{% macro validate_grain_order(metric_grain, calculation_grain) %}
    {% set grains = metrics.get_grain_order() %}
    
    {% if metric_grain not in grains or calculation_grain not in grains %}
        {% set comma = joiner(", ") %}
        {% do exceptions.raise_compiler_error("Unknown grains: [" ~ (comma() ~ metric_grain if metric_grain not in grains) ~ (comma() ~ calculation_grain if calculation_grain not in grains) ~ "]") %}
    {% endif %}

    {% set metric_grain_index = grains.index(metric_grain) %}
    {% set calculation_grain_index = grains.index(calculation_grain) %}

    {% if (calculation_grain_index < metric_grain_index) %}
        {% do exceptions.raise_compiler_error("Can't calculate secondary metric at " ~ calculation_grain ~"-level when metric is at " ~ metric_grain ~ "-level") %}
    {% endif %}
{% endmacro %}