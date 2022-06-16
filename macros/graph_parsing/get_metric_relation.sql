{% macro get_metric_relation(ref_name) %}
    
    {% if execute %}
        {% set relation = metric(ref_name)%}
        {% do return(relation) %}
    {% else %}
        {% do return(api.Relation.create()) %}
    {% endif %} 
{% endmacro %}