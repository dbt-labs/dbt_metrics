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

{% macro get_metric_calendar(ref_name) %}
    /*
        TODO: this is HORRID.
        Short version: How do we properly handle refs[0] for the metric's model, and the ref() syntax for the calendar table? 
    */

    /*
        Long version: even though the metric yml file has its model as a full ref

        - name: slack_joiners
        model: ref('dim_slack_users_2')

        the refs array from the graph contains just the string, inside a second array:

        {
        "fqn":["joel_sandbox","metrics","slack_joiners"],
        "unique_id":"metric.joel_sandbox.slack_joiners",
        "time_grains":["day", "week", "month"],
        "dimensions":["has_messaged"],
        "resource_type":"metric",
        "refs":[
            [
                "dim_slack_users_2"
            ]
        ],
        "created_at":1642578505.5324879
        }


        Whereas the calendar variable:
        vars:
            dbt_metrics_calendar_model: ref('all_days_extended_2')

        comes through as the entire ref string (it hasn't been parsed or processed yet). 
        This splits on the ' character, takes the second element, and wraps it inside an array, 
        to have the same shape as get_metric_relation expects,
        which is written to expect the metric's `model`.
    */
    
    {% set split_ref_name = ref_name.split("'")[1] %}
    {% do return(metrics.get_metric_relation([split_ref_name])) %}
{% endmacro %}