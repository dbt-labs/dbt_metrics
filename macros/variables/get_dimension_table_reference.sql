{% macro get_dimension_table_references(metric_dictionary) %}

    {% set mapping = {} %}
    {% for model_name, dim_list in metric_dictionary.dimensions.items() %}
        {% set table_reference = model_name if model_name != metric_dictionary.metric_model.name else 'base_model' %}
        {% for dim in dim_list %}
          {% do mapping.update({dim : table_reference ~ '.' ~ dim}) %}
        {% endfor %}
    {% endfor %}
    {% do return(mapping) %}
{% endmacro %}