    {# Now we see if the node already exists in the metric tree and return that if 
    it does so that we're not creating duplicates #}
    {# {%- if metric_tree[node.unique_id] is defined -%}

        {% do return(metric_tree[node.unique_id]) -%}

    {%- endif -%}

    {# {{ log("Inside Macro Depends on: " ~ node.depends_on, info=true) }} #}


    {# Here we create two sets, sets being the same as lists but they account for uniqueness. 
    One is the full set, which contains all of the parent metrics and the other is the leaf
    set, which we'll use to determine the leaf, or base metrics. #}
    {%- set full_set = [] -%}
    {%- set leaf_set = [] -%}

    {# We define parent nodes as being the parent nodes that begin with metric, which lets
    us filter out model nodes #}
    {%- set parent_nodes = node.depends_on.nodes -%}

    {%- for parent_node in parent_nodes -%}

    {# We set an if condition based on if parent nodes. If there are none, then this metric
    is a leaf node and any recursive loop should end #}
        {%- if parent_nodes -%}

            {# Now we finally recurse through the nodes. We begin by filtering the overall list we
            recurse through by limiting it to depending on metric nodes and not ALL nodes #}
            {%- for parent_id in parent_nodes -%}

                {# Then we add the parent_id of the metric to the full set. If it already existed
                then it won't make an impact but we want to make sure it is represented #}
                {%- do full_set.update(parent_id) -%}

                {# And here we re-run the current macro but fill in the parent_id so that we loop again
                with that metric information. You may be wondering, why are you using parent_id? Doesn't 
                the DAG always go from parent to child? Normally, yes! With this, no! We're reversing the 
                DAG and going up to parents to find the leaf nodes that are really parent nodes. #}
                {%- set new_parent = metrics_list[parent_id] -%}
                {%- do full_set.update(get_metric_parents(new_parent,metrics_list,metric_tree)) -%}

            {%- endfor -%}
        
        {%- else -%}

            {%- do leaf_set.update(node.unique_id) -%}

        {%- endif -%}

    {%- endfor -%}

    {%- do return(full_set) -%} #}