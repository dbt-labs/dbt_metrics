{% macro get_complete_dimension_list(metric, calendar_dimensions) %}
    
    {# Here we set the calendar as either being the default provided by the package
    or the variable provided in the project #}
    {% set calendar_dims = calendar_dimensions %}

    {# Here we are going to ensure that the metrics provided are accurate and that they are present 
    in either the metric definition or the default/custom calendar table #}
    {%- set complete_dimension_list = metric.dimensions + calendar_dimensions -%}
    {%- do return(complete_dimension_list) -%}

{% endmacro %}