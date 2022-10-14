{% macro get_model_relationships(ref_name) %}
    {% if execute %}
        {% set return_relationships = {} %}
        {% set model_ref_node = graph.nodes.values() | selectattr('name', 'equalto', ref_name) | first %}
        {% for relationship in model_ref_node.relationships %}
            {% set related_node = graph.nodes.values() | selectattr('name', 'equalto', relationship.to) | first %}
            {% set related_model_relation = api.Relation.create(
                database = related_node.database,
                schema = related_node.schema,
                identifier = related_node.alias
            ) %}
            {% do return_relationships.update(
                {relationship.to:
                {
                    'related_model_relation': related_model_relation,
                    'related_model_join_info': relationship
                }
                }
            ) %}

          
        {% endfor %}
        {% do return(return_relationships) %}
    {% else %}
        {% do return({}) %}
    {% endif %}

{% endmacro %}