{% macro get_model_relation(ref_name, metric_name) %}
    {% if execute %}
        {% set model_ref_node = graph.nodes.values() | selectattr('name', 'equalto', ref_name) | first %}
        {% if model_ref_node | length == 0 %}
            {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " is referencing the model " ~ ref_name ~ ", which does not exist.") %}
        {% endif %}

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