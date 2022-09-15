{% macro update_metric_tree(metric,metric_tree,metric_count=999)%}
    
    {# Now we see if the node already exists in the metric tree and return that if 
    it does so that we're not creating duplicates #}
    {%- if metric.name not in metric_tree|map(attribute="full_set") -%}

        {%- set full_set = metric_tree["full_set"] -%}
        {%- do full_set.append(metric.name) -%}
        {%- do metric_tree.update({'full_set':full_set}) -%}

    {%- endif -%}

    {%- do metric_tree["ordered_expression_set"].update({metric.name:metric_count}) -%}
    {%- set metric_count = metric_count - 1 -%}

    {# Here we create two sets, sets being the same as lists but they account for uniqueness. 
    One is the full set, which contains all of the parent metrics and the other is the leaf
    set, which we'll use to determine the leaf, or base metrics. #}

    {# We define parent nodes as being the parent nodes that begin with metric, which lets
    us filter out model nodes #}
    {%- set parent_metrics = metrics.get_metric_unique_id_list(metric) -%}

    {# We set an if condition based on if parent nodes. If there are none, then this metric
    is a leaf node and any recursive loop should end #}
        {%- if parent_metrics | length > 0 -%}

            {# Now we finally recurse through the nodes. We begin by filtering the overall list we
            recurse through by limiting it to depending on metric nodes and not ALL nodes #}
            {%- for parent_id in parent_metrics -%}

                {# Then we add the parent_id of the metric to the full set. If it already existed
                then it won't make an impact but we want to make sure it is represented #}
                {# {%- do full_set.append(parent_id) -%} #}
                {%- set full_set_plus = metric_tree["full_set"] -%}
                {%- if parent_id in metric_tree|map(attribute="full_set") -%}
                    {%- do full_set_plus.append(parent_id) -%}
                {%- endif -%}
                {%- do metric_tree.update({'full_set':full_set_plus}) -%}
                {# The parent_id variable here is a mapping back to the provided manifest and doesn't 
                allow for string parsing. So we create this variable to use instead #}
                {# {%- set parent_metric_name = (parent_id | string).split('.')[2] -%} #}

                {# And here we re-run the current macro but fill in the parent_id so that we loop again
                with that metric information. You may be wondering, why are you using parent_id? Doesn't 
                the DAG always go from parent to child? Normally, yes! With this, no! We're reversing the 
                DAG and going up to parents to find the leaf nodes that are really parent nodes. #}
                {%- set new_parent = metrics.get_metric_relation(parent_id) -%}

                {%- set metric_tree =  metrics.update_metric_tree(new_parent,metric_tree,metric_count) -%}

            {%- endfor -%}
        
        {%- else -%}

            {%- set parent_set_plus = metric_tree["parent_set"] -%}
            {%- if parent_id in metric_tree|map(attribute="full_set") -%}
                {%- do parent_set_plus.append(metric.name) -%}
            {%- endif -%}
            {%- do metric_tree.update({'parent_set':parent_set_plus}) -%}

        {%- endif -%}

        {%- set expression_set_plus = ( metric_tree["full_set"] | reject('in',metric_tree["parent_set"]) | list) -%}
        {%- do metric_tree.update({'expression_set':expression_set_plus}) -%}

    {%- do return(metric_tree) -%}

{% endmacro %}