{% test metric_equality(model, compare_model, compare_columns=None) %}
  {{ return(adapter.dispatch('test_metric_equality', 'metrics')(model, compare_model, compare_columns)) }}
{% endtest %}

{% macro default__test_metric_equality(model, compare_model, compare_columns=None) %}

{% set set_diff %}
    count(*) + coalesce(abs(
        sum(case when which_diff = 'a_minus_b' then 1 else 0 end) -
        sum(case when which_diff = 'b_minus_a' then 1 else 0 end)
    ), 0)
{% endset %}

{#-- Needs to be set at parse time, before we return '' below --#}
{{ config(fail_calc = set_diff) }}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

-- setup
{%- do metrics._metric_is_relation(model, 'test_metric_equality') -%}

{#-
If the compare_cols arg is provided, we can run this test without querying the
information schema — this allows the model to be an ephemeral model
-#}

{%- if not compare_columns -%}
    {%- do metrics._metric_is_ephemeral(model, 'test_metric_equality') -%}
    {%- set compare_columns = adapter.get_columns_in_relation(model) | map(attribute='quoted') -%}
{%- endif -%}

{% set compare_cols_csv = compare_columns | join(', ') %}

with a as (

    select * from {{ model }}

),

b as (

    select * from {{ compare_model }}

),

a_minus_b as (

    select {{compare_cols_csv}} from a
    {{ except() }}
    select {{compare_cols_csv}} from b

),

b_minus_a as (

    select {{compare_cols_csv}} from b
    {{ except() }}
    select {{compare_cols_csv}} from a

),

unioned as (

    select 'a_minus_b' as which_diff, a_minus_b.* from a_minus_b
    union all
    select 'b_minus_a' as which_diff, b_minus_a.* from b_minus_a

)

select * from unioned

{% endmacro %}


{% macro _metric_is_relation(obj, macro) %}
    {%- if not (obj is mapping and obj.get('metadata', {}).get('type', '').endswith('Relation')) -%}
        {%- do exceptions.raise_compiler_error("Macro " ~ macro ~ " expected a Relation but received the value: " ~ obj) -%}
    {%- endif -%}
{% endmacro %}

{% macro _metric_is_ephemeral(obj, macro) %}
    {%- if obj.is_cte -%}
        {% set ephemeral_prefix = api.Relation.add_ephemeral_prefix('') %}
        {% if obj.name.startswith(ephemeral_prefix) %}
            {% set model_name = obj.name[(ephemeral_prefix|length):] %}
        {% else %}
            {% set model_name = obj.name %}
        {%- endif -%}
        {% set error_message %}
The `{{ macro }}` macro cannot be used with ephemeral models, as it relies on the information schema.

`{{ model_name }}` is an ephemeral model. Consider making it a view or table instead.
        {% endset %}
        {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- endif -%}
{% endmacro %}