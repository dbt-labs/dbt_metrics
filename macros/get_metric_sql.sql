/*
    Core metric query generation logic.
    TODO:
      - allow start/end dates on metrics. Maybe special-case "today"?
      - allow passing in a seed with targets for a metric's value
*/


{%- macro get_metric_sql(metric, grain, dimensions, secondary_calculations, start_date, end_date, where) %}
{%- if not execute %}
    {%- do return("not execute") %}
{%- endif %}

{%- if not metric %}
    {%- do exceptions.raise_compiler_error("No metric provided") %}
{%- endif %}

{%- if not grain %}
    {%- do exceptions.raise_compiler_error("No date grain provided") %}
{%- endif %}

{% if metric.type != "expression" and metric.metrics | length > 0 %}
    {%- do exceptions.raise_compiler_error("The metric was not an expression and dependent on another metric. This is not currently supported - if this metric depends on another metric, please change the type to expression.") %}
{%- endif %}

{# TODO Change so that the filter condition is whether metrics are present in manifest #}
{%- if metric.metrics | length > 0 %}

    {# First we get the list of nodes that this metric is dependent on. This is inclusive 
    of all parent metrics and SHOULD only contain parent metrics #}
    {%- set node_list = metric.depends_on.nodes -%}
    {%- set metric_list = [] -%}

    {# This part is suboptimal - we're looping through the dependent nodes and extracting
    the model name from the idenitfier. Ideally we'd just use the metrics attribute but 
    right now its a list of lists #}
    {%- for node in node_list -%}  
        {% set metric_name = node.split('.')[2] %}
        {% do metric_list.append(metric_name) %}
    {%- endfor -%}

{% else %}

    {# For non-expression metrics, we just need the relation of the base model ie 
    the model that its built. Then we append it to the metric list name so the same
    variable used in expression metrics can be used below #}
    {%- set metric_list = [] -%}
    {%- set base_model = metric.model.split('\'')[1]  -%}
    {%- set model = metrics.get_model_relation(base_model if execute else "") %}
    {% do metric_list.append(metric.name) %}

{%- endif %}

{%- set dimensions=metrics.get_dimension_list(dimensions) -%}

{# ############
LET THE COMPOSITION BEGIN!
############ #}

{# First we add the calendar table - we only need to do this once no matter how many
metrics there are #}
{{metrics.gen_calendar_cte(calendar_tbl,start_date,end_date)}}

{# Next we check if it is a composite metric or single metric by checking the length of the list#}
{# This filter forms the basis of how we construct the SQL #}
{%- if metric_list|length > 1 -%}
    
    {# If composite, we begin by looping through each of the metric names that make
    up the composite metric. #}
    {%for metric_object in metric_list%}
        {%- set loop_metric = metrics.get_metric_relation(metric_object) -%}
        {%- set base_model = loop_metric.model.split('\'')[1]  -%}
        {%- set model = metrics.get_model_relation(base_model if execute else "") %}
        {{ metrics.build_metric_sql(loop_metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl) }}
        {% if loop.last %}
            {{ metrics.gen_joined_metrics_cte(metric, grain, dimensions) }}
            {{ metrics.gen_secondary_calculation_cte(metric,dimensions,grain,secondary_calculations) }}
            {{ metrics.get_final_cte(metric,grain,secondary_calculations) }}
        {% endif %}
    {%- endfor -%}
    
    {# If it is NOT a composite metric, we run the baseline model #}
    {%- else -%}
        {{ metrics.build_metric_sql(metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl) }}
        {{ metrics.gen_secondary_calculation_cte(metric,dimensions,grain,secondary_calculations) }}
        {{ metrics.get_final_cte(metric,grain,secondary_calculations) }}

{%- endif -%}


{% endmacro %}