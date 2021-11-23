
--TODO: there would need to be some way to catch bad input (e.g. non-existent `how` or `aggregate`)
-- I also don't like how much I have to pass around metric_name, but maybe that's unavoidable

{% macro aggregate_primary_metric(aggregate, expression) %}
    {{ return(adapter.dispatch('aggregate_primary_metric')(aggregate, expression)) }}
{% endmacro %}

-- Discuss: I'm open to this intermediary macro not existing, 
-- and aggregate_primary_metric just calling dispatch() for metric_* directly. 
-- I've tried that in secondary_calculations, and I think I like it better
{% macro default__aggregate_primary_metric(aggregate, expression) %}
    {{ return(adapter.dispatch('metric_' ~ aggregate)(expression)) }}
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

-------------------------------------------------------------


{% macro secondary_calculations(metric_name, aggregate, dims, config) %}
    {{ return(adapter.dispatch('metric_secondary_calculations_' ~ config.type)(metric_name, aggregate, dims, config)) }}
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
    
    --The how component could either work like this... 
    {% if config.how == 'difference' %}
        coalesce({{ metric_name }}, 0) - coalesce({{ calc_sql }}, 0)
    {% elif config.how == 'ratio' %}
        coalesce({{ metric_name }}, 0) / nullif({{ calc_sql }}, 0)::float
    {% else %}
        {% do exceptions.raise_compiler_error("Bad 'how' for period_over_period: " ~ calc.how) %}
    {% endif %}

{% endmacro %}

{% macro default__metric_secondary_calculations_rolling(metric_name, aggregate, dims, config) %}
    {% set calc_sql %}
        {{ adapter.dispatch('metric_' ~ aggregate)(expression) }}
        over (
            {% if dims -%}
                partition by {{ dims | join(", ") }} 
            {% endif -%}
            order by period
        )
        rows between {{ config.window - 1 }} preceding and current row
    {% endset %}

    -- ... or like this, which adds another hop for people to follow but would be DRYer
    {{ adapter.dispatch('metric_how_' ~ config.how)(metric_name, calc_sql) }}
{% endmacro %}

{% macro default__metric_how_difference(metric_name, calc_sql) %}
    coalesce({{ metric_name }}, 0) - coalesce({{ calc_sql }}, 0)
{% endmacro %}

{% macro default__metric_how_ratio(metric_name, calc_sql) %}
    coalesce({{ metric_name }}, 0) / nullif({{ calc_sql }}, 0)::float
{% endmacro %}