
--TODO: there would need to be some way to catch bad input (e.g. non-existent `how` or `aggregate`)
-- I also don't like how much I have to pass around metric_name, but maybe that's unavoidable

--TODO: Do we have a list of aggregations that we're supporting? 
{% macro aggregate_primary_metric(aggregate, expression) %}
    {{ return(adapter.dispatch('aggregate_primary_metric')(aggregate, expression)) }}
{% endmacro %}

-- Discuss: I'm open to this intermediary macro not existing, 
-- and aggregate_primary_metric just calling dispatch() for metric_* directly. 
-- Would that break others' ability to override this?
{% macro default__aggregate_primary_metric(aggregate, expression) %}
    {% if aggregate == 'count' %}
        {{ return(adapter.dispatch('metric_count')(expression)) }}
    
    {% elif aggregate == 'count_distinct' %}
        {{ return(adapter.dispatch('metric_count_distinct')(expression)) }}
    
    {% elif aggregate == 'average' %}
        {{ return(adapter.dispatch('metric_average')(expression)) }}
    
    {% elif aggregate == 'max' %}
        {{ return(adapter.dispatch('metric_max')(expression)) }}
    
    {% elif aggregate == 'sum' %}
        {{ return(adapter.dispatch('metric_sum')(expression)) }}
    
    {% else %}
        {% do exceptions.raise_compiler_error("Unknown aggregation style: " ~ aggregate) %}  
    {% endif %}
{% endmacro %}

{% macro default__metric_count(expression) %}
    count({{ expression }})
{% endmacro %}

{% macro default__metric_count_distinct(expression) %}
    count(distinct {{ expression }})
{% endmacro %}

{% macro default__metric_average(expression) %}
    avg({{ expression }})
{% endmacro %}

{% macro default__metric_max(expression) %}
    max({{ expression }})
{% endmacro %}

{% macro default__metric_sum(expression) %}
    sum({{ expression }})
{% endmacro %}

-------------------------------------------------------------

{% macro metric_secondary_calculations(metric_name, aggregate, dims, config) %}
    {{ return(adapter.dispatch('metric_secondary_calculations')(metric_name, aggregate, dims, config)) }}
{% endmacro %}

{% macro default__metric_secondary_calculations(metric_name, aggregate, dims, config) %}
    {% set calc_type = config.type %}
    {% set calc_sql = '' %}
    
    {% if calc_type == 'period_over_period' %}
        {% set calc_sql = adapter.dispatch('metric_secondary_calculations_period_over_period')(metric_name, aggregate, dims, config) %}
   
    {% elif calc_type == 'rolling' %}
        {% set calc_sql = adapter.dispatch('metric_secondary_calculations_rolling')(metric_name, aggregate, dims, config) %}
    
    {% elif calc_type == 'period_to_date' %}
        {% set calc_sql = adapter.dispatch('metric_secondary_calculations_period_to_date')(metric_name, aggregate, dims, config) %}
    
    {% else %}
        {% do exceptions.raise_compiler_error("Unknown secondary calculation: " ~ calc_type) %}  
    {% endif %}

    {{ calc_sql }}

{% endmacro %}

{% macro default__metric_secondary_calculations_period_over_period(metric_name, aggregate, dims, config) %}
    {% set calc_sql %}
        lag(
            {{- metric_name }}, {{ config.lag -}}
        ) over (
            {% if dims -%}
                partition by {{ dims | join(", ") }} 
            {% endif -%}
            order by period
        )
    {% endset %}
    

    {% if config.how == 'difference' %}
        {% do return (adapter.dispatch('metric_how_difference')(metric_name, calc_sql)) %}
    
    {% elif config.how == 'ratio' %}
        {% do return (adapter.dispatch('metric_how_ratio')(metric_name, calc_sql)) %}
    
    {% else %}
        {% do exceptions.raise_compiler_error("Bad 'how' for period_over_period: " ~ config.how) %}
    {% endif %}

{% endmacro %}

{% macro default__metric_secondary_calculations_rolling(metric_name, aggregate, dims, config) %}
    {% set calc_sql %}
        {{ adapter.dispatch('aggregate_primary_metric')(aggregate, metric_name) }}
        over (
            {% if dims -%}
                partition by {{ dims | join(", ") }} 
            {% endif -%}
            order by period
        )
        rows between {{ config.window - 1 }} preceding and current row
    {% endset %}

    {% do return (calc_sql) %}

{% endmacro %}

{% macro default__metric_secondary_calculations_period_to_date(metric_name, aggregate, dims, config) %}
    {% set calc_sql %}
        {{ adapter.dispatch('aggregate_primary_metric')(aggregate, metric_name) }}
        over (
            partition by date_{{ config.period }}
            {% if dims -%}
                , {{ dims | join(", ") }}
            {%- endif %}
            order by period
        )
        rows between unbounded preceding and current row
    {% endset %}

    {% do return (calc_sql) %}
    
{% endmacro %}



{% macro default__metric_how_difference(metric_name, calc_sql) %}
    coalesce({{ metric_name }}, 0) - coalesce({{ calc_sql }}, 0)
{% endmacro %}

{% macro default__metric_how_ratio(metric_name, calc_sql) %}
    coalesce({{ metric_name }}, 0) / nullif({{ calc_sql }}, 0)::float
{% endmacro %}