{#-
    Future tests:
    - Expect failure when missing arguments
-#}

{%- set test_cases =
    [
        {"input_object": metrics.period_over_period(alias='pop_constructor_named_args_manual_alias', comparison_strategy='difference', interval=1), "expected_alias": 'pop_constructor_named_args_manual_alias'},
        {"input_object": metrics.period_over_period('ratio', 2, 'pop_constructor_positional_args_manual_alias'), "expected_alias": 'pop_constructor_positional_args_manual_alias'},
        {"input_object": {"calculation": "period_over_period", "comparison_strategy": "difference", "interval": 3, "alias": "pop_object_manual_alias"}, "expected_alias": "pop_object_manual_alias"},
        {"input_object": metrics.period_over_period(interval=4, comparison_strategy='difference'), "expected_alias": 'difference_to_4_fortnight_ago'},
        {"input_object": metrics.period_over_period('ratio', 5), "expected_alias": 'ratio_to_5_fortnight_ago'},
        {"input_object": {"calculation": "period_over_period", "comparison_strategy": "difference", "interval": 6}, "expected_alias": "difference_to_6_fortnight_ago"},
        
        {"input_object": metrics.period_to_date(alias='ptd_constructor_named_args_manual_alias', aggregate='average', period="day"), "expected_alias": 'ptd_constructor_named_args_manual_alias'},
        {"input_object": metrics.period_to_date('average', "week", 'ptd_constructor_positional_args_manual_alias'), "expected_alias": 'ptd_constructor_positional_args_manual_alias'},
        {"input_object": {"calculation": "period_to_date", "aggregate": "average", "period": "month", "alias": "ptd_object_manual_alias"}, "expected_alias": "ptd_object_manual_alias"},
        {"input_object": metrics.period_to_date(period="quarter", aggregate='average'), "expected_alias": 'average_for_quarter'},
        {"input_object": metrics.period_to_date('average', 'year'), "expected_alias": 'average_for_year'},
        {"input_object": {"calculation": "period_to_date", "aggregate": "min", "period": "week"}, "expected_alias": "min_for_week"},
        
        {"input_object": metrics.rolling(alias='rolling_constructor_named_args_manual_alias', aggregate='min', interval=1), "expected_alias": 'rolling_constructor_named_args_manual_alias'},
        {"input_object": metrics.rolling('min', 2, 'rolling_constructor_positional_args_manual_alias'), "expected_alias": 'rolling_constructor_positional_args_manual_alias'},
        {"input_object": {"calculation": "rolling", "aggregate": "min", "interval": 3, "alias": "rolling_object_manual_alias"}, "expected_alias": "rolling_object_manual_alias"},
        {"input_object": metrics.rolling(interval=4, aggregate='min'), "expected_alias": 'rolling_min_4_fortnight'},
        {"input_object": metrics.rolling('min', 5), "expected_alias": 'rolling_min_5_fortnight'},
        {"input_object": {"calculation": "rolling", "aggregate": "min", "interval": 6}, "expected_alias": "rolling_min_6_fortnight"},
        
        
    ]
-%}
with reality_vs_expectations as (
    {%- for test_case in test_cases %}
        select '{{ metrics.generate_secondary_calculation_alias(test_case["input_object"], "fortnight") }}' as generated_alias, '{{ test_case["expected_alias"] }}' as expected_alias
        {% if not loop.last %}
        union all 
        {% endif %}
    {% endfor %}
)

select * 
from reality_vs_expectations
where generated_alias != expected_alias