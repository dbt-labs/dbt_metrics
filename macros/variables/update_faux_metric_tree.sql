{% macro update_faux_metric_tree(metric_definition, metric_tree, develop_yml, metric_count=999)%}
    

    {# Now we see if the node already exists in the metric tree and return that if 
    it does so that we're not creating duplicates #}
    {%- if metric_definition.name not in metric_tree|map(attribute="full_set") -%}

        {%- set full_set = metric_tree["full_set"] -%}
        {%- do full_set.append(metric_definition.name) -%}
        {%- do metric_tree.update({'full_set':full_set}) -%}

    {%- endif -%}

    {# Here we're starting with the highest level and assigning the metric tree that first level
    value. This is used before de-duping in get_faux_metric_tree #}
    {%- do metric_tree["ordered_expression_set"].update({metric_definition.name:metric_count}) -%}
    {%- set metric_count = metric_count - 1 -%}

    {# Here we create two sets, sets being the same as lists but they account for uniqueness. 
    One is the full set, which contains all of the parent metrics and the other is the leaf
    set, which we'll use to determine the leaf, or base metrics. #}

    {% set develop_metric_list = [] %}
    {% for develop_metric_name in develop_yml %}
        {% do develop_metric_list.append(develop_metric_name) %}
    {% endfor %}

    {# We define parent nodes as being the parent nodes that begin with metric, which lets
    us filter out model nodes #}
    {%- set parent_metrics = metrics.get_develop_unique_metric_id_list(metric_definition) -%}
    {# We set an if condition based on if parent nodes. If there are none, then this metric
    is a leaf node and any recursive loop should end #}
    {%- if parent_metrics | length > 0 -%}

        {# Now we finally recurse through the nodes. We begin by filtering the overall list we
        recurse through by limiting it to depending on metric nodes and not ALL nodes #}
        {%- for parent_metric_name in parent_metrics -%}

            {# Then we add the parent_id of the metric to the full set. If it already existed
            then it won't make an impact but we want to make sure it is represented. Will dedupe
            in final macro #}
            {%- set full_set_plus = metric_tree["full_set"] -%}
            {%- if parent_metric_name in metric_tree|map(attribute="full_set") -%}
                {%- do full_set_plus.append(parent_metric_name) -%}
            {%- endif -%}
            {%- do metric_tree.update({'full_set':full_set_plus}) -%}

            {# And here we re-run the current macro but fill in the parent_id so that we loop again
            with that metric information. You may be wondering, why are you using parent_id? Doesn't 
            the DAG always go from parent to child? Normally, yes! With this, no! We're reversing the 
            DAG and going up to parents to find the leaf nodes that are really parent nodes. #}
            
            {# So here we need to test if the parent id/metric name exists in the manifest OR in
            the develop yml. Manifest takes priority and then defaults back to yml if not present #}
            {% if parent_metric_name in develop_metric_list and parent_metric_name is not none %}
                {% set parent_metric_definition = develop_yml[parent_metric_name] %}
            {% else %}
                {%- set parent_metric_definition = metrics.get_metric_relation(parent_metric_name) -%}
            {% endif %}

            {%- set metric_tree =  metrics.update_faux_metric_tree(parent_metric_definition, metric_tree, develop_yml, metric_count) -%}

        {%- endfor -%}
    
    {%- else -%}

        {%- set parent_set_plus = metric_tree["parent_set"] -%}
        {%- do parent_set_plus.append(metric_definition.name) -%}
        {%- do metric_tree.update({'parent_set':parent_set_plus}) -%}

    {%- endif -%}

    {%- set expression_set_plus = ( metric_tree["full_set"] | reject('in',metric_tree["parent_set"]) | list) -%}
    {%- do metric_tree.update({'expression_set':expression_set_plus}) -%}

    {%- do return(metric_tree) -%}

{% endmacro %}