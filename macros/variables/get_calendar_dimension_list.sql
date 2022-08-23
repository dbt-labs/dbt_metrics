{% macro get_calendar_dimension_list() %}
    
    {% set calendar_dimensions = var('custom_calendar_dimension_list',[]) %}
    {% do return(calendar_dimensions) %}

{% endmacro %}