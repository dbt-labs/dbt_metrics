{%- macro gen_property_to_aggregate(metric_dictionary, grain, dimensions, calendar_dimensions) -%}
    {{ return(adapter.dispatch('gen_property_to_aggregate', 'metrics')(metric_dictionary, grain, dimensions, calendar_dimensions)) }}
{%- endmacro -%}

{% macro default__gen_property_to_aggregate(metric_dictionary, grain, dimensions, calendar_dimensions) %}
    {% if metric_dictionary.calculation_method == 'median' -%}
        {{ return(adapter.dispatch('property_to_aggregate_median', 'metrics')(metric_dictionary, grain, dimensions, calendar_dimensions)) }}

    {% elif metric_dictionary.calculation_method == 'count' -%}
        {{ return(adapter.dispatch('property_to_aggregate_count', 'metrics')(metric_dictionary)) }}

    {% elif metric_dictionary.expression and metric_dictionary.expression | replace('*', '') | trim != '' %}
        {{ return(adapter.dispatch('property_to_aggregate_default', 'metrics')(metric_dictionary)) }}

    {% else %}
        {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric_dictionary.name ~ "`") -%}  
    {% endif %}

{%- endmacro -%}

{% macro default__property_to_aggregate_median(metric_dictionary, grain, dimensions, calendar_dimensions) %}
            ({{metric_dictionary.expression }}) as property_to_aggregate__{{metric_dictionary.name}}
{%- endmacro -%}

{% macro bigquery__property_to_aggregate_median(metric_dictionary, grain, dimensions, calendar_dimensions) %}

            percentile_cont({{metric_dictionary.expression }}, 0.5) over (
                {% if grain or dimensions | length > 0 or calendar_dimensions | length > 0 -%}
                partition by 
                {% if grain -%}
                calendar.date_{{ grain }}
                {%- endif %}
                {% for dim in dimensions -%}
                    {%- if loop.first and not grain-%}
                base_model.{{ dim }}
                    {%- else -%}
                ,base_model.{{ dim }}
                    {%- endif -%}
                {%- endfor -%}
                {% for calendar_dim in calendar_dimensions -%}
                    {%- if loop.first and dimensions | length == 0 and not grain %}
                calendar.{{ calendar_dim }}
                    {%else -%}
                ,calendar.{{ calendar_dim }}
                    {%- endif -%}
                {%- endfor %}
                {%- endif %}
            ) as property_to_aggregate__{{metric_dictionary.name}}

{%- endmacro -%}

{% macro default__property_to_aggregate_count(metric_dictionary) %}
            1 as property_to_aggregate__{{metric_dictionary.name}}
{%- endmacro -%}

{% macro default__property_to_aggregate_default(metric_dictionary) %}
            ({{metric_dictionary.expression }}) as property_to_aggregate__{{metric_dictionary.name}}
{%- endmacro -%}