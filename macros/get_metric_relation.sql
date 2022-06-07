{% macro get_metric_relation(ref_name) %}
    {% if execute %}
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