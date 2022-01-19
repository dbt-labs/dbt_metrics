{% macro get_metric_relation(ref_name) %}
    {% if execute %}
        /* TODO: How do we properly handle refs[0] for the metric's model, and the ref() syntax for the calendar table? */
        {% set model_ref_node = graph.nodes.values() | selectattr('name', 'equalto', ref_name[0]) | first %}
        {% set relation = api.Relation.create(
            database = model_ref_node.database,
            schema = model_ref_node.schema,
            identifier = model_ref_node.alias
        )
        %}
        {% do return(relation) %}
    {% else %}
        {% do return(api.Relation.create()) %}
    {% endif %} 
{% endmacro %}

{% macro get_metric_calendar(ref_name) %}
    --TODO: this is HORRID.
    {% do return(metrics.get_metric_relation([(ref_name.split("'")[1])])) %}
{% endmacro %}