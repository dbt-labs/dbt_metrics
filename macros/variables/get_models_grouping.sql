{%- macro get_models_grouping(metric_tree, metrics_dictionary) -%}
{#- 
The purpose of this macro is to create a dictionary that can be used by
gen_base_query and gen_aggregate_query in order to intelligently group
metrics together on whether they can be queried in the same query. These
will be grouped together with a unique model name as the key and the value 
containing the list of the metrics. This is complicated because we allow
different properties that affect the base query, so we can't do a single 
grouping based on model. As such, if a metric contains one of these properties
we have to create a group for that specific combination.

The properties that cause us to group the metric seperately are:
    - windows
    - filters
    - timestamp fields

In order to ensure consistency, we will also include those values in the 
dictionary so we can reference them from the metrics grouping (ie a single
location) instead of from a randomly selected metric in the list of metrics.

An example output looks like:
{
    'model_4f977327f02b5c04af4337f54ed81a17': {
        'metric_names':['metric_a','metric_b'],
        'metric_timestamp': order_date,
        'metric_filters':[
            MetricFilter(field='had_discount', operator='is', value='true'), 
            MetricFilter(field='order_country', operator='=', value='CA')
        ]
        'metric_window': MetricTime(count=14, period=<MetricTimePeriod.month: 'month'>)
    }
} 
 -#}

    {% set models_grouping = {} %}

    {% for metric_name in metric_tree.parent_set %}
        {% set metric_dictionary = metrics_dictionary[metric_name] %}

        {% set models_grouping = metrics.get_model_group(
                models_grouping=models_grouping,
                metric_model=metric_dictionary.metric_model,
                metric_model_name=metric_dictionary.metric_model_name,
                metric_name=metric_dictionary.name,
                metric_timestamp=metric_dictionary.timestamp,
                metric_filters=metric_dictionary.filters,
                metric_window=metric_dictionary.window
            ) %}

    {% endfor %}

    {% do return(models_grouping) %}

{%- endmacro -%}