{% macro cast_column_for_join(qualified_column) %}

    {{ return(adapter.dispatch('cast_column_for_join', 'metrics')(qualified_column)) }}

{% endmacro %}

{% macro default__cast_column_for_join(qualified_column) %}
    coalesce(cast({{ qualified_column }} as {{ dbt_utils.type_string() }}), '_dbt_metrics_null_value_')
{% endmacro %}

{% macro redshift__cast_column_for_join(qualified_column) %}
    coalesce(cast(
        case 
            when {{ qualified_column }} is true then 'true' 
            when {{ qualified_column }} is false then 'false' 
            else {{ qualified_column }} 
        end as {{ dbt_utils.type_string() }}
    ), '_dbt_metrics_null_value_')
{% endmacro %}