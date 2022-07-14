{% macro get_common_valid_dimension_list(dimensions,full_set) %}
    
    {# First we create an empty dictionary to store information as we loop through 
    the dimension provided in the macro #}
    {% set common_valid_dimension_dict = {} %}
    {% for dim in dimensions %}

        {# We create a base key value pair in the dictionary that has a base value of 0.
        This value is later used downstream to match the number of metrics in the full set
        and only include the dimension if the counts match #}
        {% do common_valid_dimension_dict.update({dim:0})%}

        {# Now we loop through all the metrics in the full set, which is all metrics, parent metrics,
        and expression metrics associated with the macro call #}
        {% for metric_name in full_set %}
            {% set metric_relation = metric(metric_name)%}
            
            {# This macro returns a list of dimensions that are inclusive of calendar dimensions #}
            {% set complete_dimension_list = metrics.get_complete_dimension_list(metric_relation)%}

            {# If the dimension provided is not present in the loop metrics dimension list then we 
            will raise an error. If it is missing in ANY of the metrics, it cannot be used in the 
            macro call. Only dimensions that are valid in all metrics are valid in the macro call #}
            {% if dim not in complete_dimension_list %}
                {%- do exceptions.raise_compiler_error("The dimension " ~ dim ~ " is not an approved dimension for the metric " ~ metric_relation.name) %}
            {% else %}
                {# Here we update the value of the dictionary value to be + 1 to the previous value #}
                {% set new_dim_value = common_valid_dimension_dict[dim] + 1 %}
                {% do common_valid_dimension_dict.update({dim:new_dim_value})%}
            {% endif %}

        {%endfor%}
    {%endfor%}

    {# We create an empty list that we later return at the end of the macro #}
    {% set common_valid_dimension_list = [] %}
    
    {# Here we iterate through the dictionary and for each dimension provided we check 
    if the dimension is present the number of times that is equal to the total number 
    of metrics used. If they match, we append it to the list. #}
    {% for key, value in common_valid_dimension_dict.items() %}
        {% if full_set | length == value %}
            {% do common_valid_dimension_list.append(key) %}
        {% endif %}
    {% endfor %}

    {# Return the list!  #}
    {% do return(common_valid_dimension_list) %}

{% endmacro %}