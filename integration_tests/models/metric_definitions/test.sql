{# select
    date_month,
    had_discount,
    percentile_cont(property_to_aggregate,0.5) over () as base_median_metric,
    logical_or(metric_date_day is not null) as has_data
from (
    select 
        cast(base_model.order_date as date) as metric_date_day, -- timestamp field
        calendar_table.date_month as date_month,
        calendar_table.date_day as window_filter_date,
        base_model.had_discount,
        (discount_total) as property_to_aggregate
    from `dbt-dev-168022`.`dbt_metrics`.`fact_orders` base_model 
    left join `dbt-dev-168022`.`dbt_metrics`.`custom_calendar` calendar_table
        on cast(base_model.order_date as date) = calendar_table.date_day
        where 1=1
    ) as base_query

    where 1=1
    group by 1, 2
; #}
{# select
    date_month,
    had_discount,
    any_value(property_to_aggregate) as base_median_metric,
    logical_or(metric_date_day is not null) as has_data
from ( #}
    select 
        cast(base_model.order_date as date) as metric_date_day, -- timestamp field
        calendar_table.date_month as date_month,
        calendar_table.date_day as window_filter_date,
        base_model.had_discount,
        percentile_cont(discount_total, 0.5) over (partition by base_model.had_discount, calendar_table.date_month) as property_to_aggregate
    from `dbt-dev-168022`.`dbt_metrics`.`fact_orders` base_model 
    left join `dbt-dev-168022`.`dbt_metrics`.`custom_calendar` calendar_table
        on cast(base_model.order_date as date) = calendar_table.date_day
        where 1=1
    {# ) as base_query

    where 1=1
    group by 1, 2 #}