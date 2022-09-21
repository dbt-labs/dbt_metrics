# dbt_metrics
<!--This table of contents is automatically generated. Any manual changes between the ts and te tags will be overridden!-->
<!--ts-->
* [dbt_metrics](#dbt_metrics)
* [About](#about)
   * [Tenets](#tenets)
   * [Installation Instructions](#installation-instructions)
* [Macros](#macros)
   * [Calculate](#calculate)
      * [Migration from metric to calculate](#migration-from-metric-to-calculate)
   * [Develop](#develop)
* [Use cases and examples](#use-cases-and-examples)
   * [Jaffle Shop Metrics](#jaffle-shop-metrics)
   * [Inside of dbt Models](#inside-of-dbt-models)
   * [Via the interactive dbt server (coming soon)](#via-the-interactive-dbt-server-coming-soon)
* [Secondary calculations](#secondary-calculations)
   * [Period over Period (<a href="/macros/secondary_calculations/secondary_calculation_period_over_period.sql">source</a>)](#period-over-period-source)
   * [Period to Date (<a href="/macros/secondary_calculations/secondary_calculation_period_to_date.sql">source</a>)](#period-to-date-source)
   * [Rolling (<a href="/macros/secondary_calculations/secondary_calculation_rolling.sql">source</a>)](#rolling-source)
* [Customisation](#customisation)
   * [Expression Metrics](#expression-metrics)
   * [Multiple Metrics](#multiple-metrics)
   * [Where Clauses](#where-clauses)
   * [Calendar](#calendar)
      * [Dimensions from calendar tables](#dimensions-from-calendar-tables)
   * [Time Grains](#time-grains)
   * [Custom aggregations](#custom-aggregations)
   * [Secondary calculation column aliases](#secondary-calculation-column-aliases)

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->
<!-- Added by: runner, at: Wed Sep 21 14:06:18 UTC 2022 -->

<!--te-->



# About
This dbt package generates queries based on [metrics](https://docs.getdbt.com/docs/building-a-dbt-project/metrics), introduced to dbt Core in v1.0. For more information on metrics, such as available types, properties, and other definition parameters, please reference the documentation linked above.

## Tenets
The tenets of `dbt_metrics`, which should be considered during development, issues, and contributions, are:
- A metric value should be consistent everywhere that it is referenced
- We prefer generalized metrics with many dimensions over specific metrics with few dimensions
- It should be easier to use dbt’s metrics than it is to avoid them
- Organization and discoverability are as important as precision
- One-off models built to power metrics are an anti-pattern

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/dbt-labs/metrics/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `package.yml`

```yaml
packages:
  - package: dbt-labs/metrics
    version: [">=0.3.0", "<0.4.0"]
```

# Macros

## Calculate
The calculate macro performs the metric aggregation and returns the dataset based on the specifications
of the metric definition and the options selected in the macro. It can be accessed [like any other macro](https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros#using-a-macro-from-a-package): 

```sql
select * 
from {{ metrics.calculate(
    metric('new_customers'),
    grain='week',
    dimensions=['plan', 'country'],
    secondary_calculations=[
        metrics.period_over_period(comparison_strategy="ratio", interval=1, alias="pop_1wk"),
        metrics.period_over_period(comparison_strategy="difference", interval=1),

        metrics.period_to_date(aggregate="average", period="month", alias="this_month_average"),
        metrics.period_to_date(aggregate="sum", period="year"),

        metrics.rolling(aggregate="average", interval=4, alias="avg_past_4wks"),
        metrics.rolling(aggregate="min", interval=4)
    ],
    start_date='2022-01-01',
    end_date='2022-12-31',
    where="some_column='filter_value'"
) }}
```

`start_date` and `end_date` are optional. When not provided, the spine will span all dates from oldest to newest in the metric's dataset. This default is likely to be correct in most cases, but you can use the arguments to either narrow the resulting table or expand it (e.g. if there was no new customers until 3 January but you want to include the first two days as well). Both values are inclusive.

### Migration from metric to calculate
In version `0.3.0` of the dbt_metrics package, the name of the main macro was changed from `metric` to `calculate`. This was done in order to better reflect the work being performed by the macro and match the semantic naming followed by the rest of the macros in the package (describing the action, not the output). Additionally, the `metric_name` input was changed to take a single `metric` function or multiple `metric` functions provided in a list.

To correctly change this syntax, you must:
- change `metrics.metric` to `metrics.calculate`.
- change `metric_name` to `metric('name_here')` 
  - alternatively use `[metric('name_here'),metric('another_name_here')]` for multiple metrics

## Develop
There are times when you want to test what a metric might look like before defining it in your project. In these cases you should use the `develop` metric, which allows you to provide a single metric in a contained yml in order to simulate what the metric might loook like if defined in your project.

**Limitations:**
- The provided yml can only contain one metric
- The metric in question cannot be an expression metric

```sql
{% set my_metric_yml -%}

metrics:
  - name: develop_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: average
    sql: discount_total
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        grain='month'
        )
    }}
```

# Use cases and examples

## Jaffle Shop Metrics
For those curious about how to implement metrics in a dbt project, please reference the [`jaffle_shop_metrics`](https://github.com/dbt-labs/jaffle_shop_metrics). 

## Inside of dbt Models
You may want to materialize the results as a fixed table for querying. This is not the way we expect the dbt Metrics layer to add the most value, but is a way to experiment with the project without needing access to the interactive server.  

## Via the interactive dbt server (coming soon)
When [dbt server](https://blog.getdbt.com/licensing-dbt/) is released in late 2022, you will be able to access these macros interactively, without needing to build each variant as a single dbt model. For more information, check out the [keynote presentation from Coalesce 2021](https://www.getdbt.com/coalesce-2021/keynote-the-metrics-system/).

# Secondary calculations
Secondary calculations are window functions which act on the primary metric or metrics. You can use them to compare values to an earlier period and calculate year-to-date sums or rolling averages.

Create secondary calculations using the convenience [constructor](https://en.wikipedia.org/wiki/Constructor_(object-oriented_programming)) macros. Alternatively, you can manually create a list of dictionary entries (not recommended).

<details> <summary> Example of manual dictionary creation (not recommended) </summary>

  Creating a calculation this way has no input validation. 
  ```python
  [
      {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "difference", "alias": "pop_1mth"},
      {"calculation": "rolling", "interval": 3, "aggregate": "sum"}
  ]
  ```
  
</details>

Column aliases are [automatically generated](#secondary-calculation-column-aliases), but you can override them by setting `alias`. 

## Period over Period ([source](/macros/secondary_calculations/secondary_calculation_period_over_period.sql))
The period over period secondary calculation performs a calculation against the metric(s) in question by either determining the difference or the ratio between two points of time. This other point in time is determined by the input variable which looks at the grain selected in the macro. 

Constructor: `metrics.period_over_period(comparison_strategy, interval [, alias, metric_list])`

| Parameter                  | Example | Description | Required |
| -------------------------- | ----------- | ----------- | -----------|
| `comparison_strategy`      | `ratio` or `difference` | How to calculate the delta between the two periods | Yes |
| `interval`                 | 1 | Integer - the number of time grains to look back | Yes |
| `alias`                    | 'week_over_week' | The column alias for the resulting calculation | No |
| `metric_list`              | 'base_sum_metric' | List of metrics that the secondary calculation should be applied to. Default is all metrics selected | No |

## Period to Date ([source](/macros/secondary_calculations/secondary_calculation_period_to_date.sql))
The period to date secondary calculation performs an aggregation on a defined **period** of time that is equal to or coarser (higher, more aggregated) than the grain selected. Great example of this is when you want to display a month_to_date value alongside your weekly grained metric.

Constructor: `metrics.period_to_date(aggregate, period [, alias, metric_list])`

| Parameter                  | Example | Description | Required |
| -------------------------- | ----------- | ----------- | -----------|
| `aggregate`                | `max`, `average` | The aggregation to use in the window function. Options vary based on the primary aggregation and are enforced in [validate_aggregate_coherence()](/macros/secondary_calculations/validate_aggregate_coherence.sql). | Yes |
| `period`                   | `"day"`, `"week"` | The time grain to aggregate to. One of [`"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`]. Must be at equal or coarser (higher, more aggregated) granularity than the metric's grain (see [Time Grains](#time-grains) below). In example grain of `month`, the acceptable periods would be `month`, `quarter`, or `year`. | Yes |
| `alias`                    | 'month_to_date' | The column alias for the resulting calculation | No |
| `metric_list`              | 'base_sum_metric' | List of metrics that the secondary calculation should be applied to. Default is all metrics selected | No |

## Rolling ([source](/macros/secondary_calculations/secondary_calculation_rolling.sql))
The rolling secondary calculation performs an aggregation on a defined number of rows in metric dataset. For example, if the user selects the `week` grain and sets a rolling secondary calculation to `4` then the value returned will be a rolling 4 week calculation of whatever aggregation type was selected.

Constructor: `metrics.rolling(aggregate, interval [, alias, metric_list])`

| Parameter                  | Example | Description | Required |
| -------------------------- | ----------- | ----------- | -----------|
| `aggregate`                | `max`, `average` | The aggregation to use in the window function. Options vary based on the primary aggregation and are enforced in [validate_aggregate_coherence()](/macros/secondary_calculations/validate_aggregate_coherence.sql). | Yes |
| `interval`                 | 1 | Integer - the number of time grains to look back | Yes |
| `alias`                    | 'month_to_date' | The column alias for the resulting calculation | No |
| `metric_list`              | 'base_sum_metric' | List of metrics that the secondary calculation should be applied to. Default is all metrics selected | No |

# Customisation
Most behaviour in the package can be overridden or customised.

## Expression Metrics 
Version `0.3.0` of this package, and beyond, offer support for `expression` metrics! More information around this type can be found in the[`metrics` page of dbt docs](https://docs.getdbt.com/docs/building-a-dbt-project/metrics)/.

## Multiple Metrics
There may be instances where you want to return multiple metrics within a single macro. This is possible by providing a list of metrics instead of a single metric. See example below:

```sql
  select *
  from 
  {{ metrics.calculate(
      [metric('base_sum_metric'), metric('base_average_metric')], 
      grain='day', 
      dimensions=['had_discount']
  }}
```

**Note**: The metrics must share the `time_grain` selected in the macro AND the `dimensions` selected in the macro. If these are not shared between the 2+ metrics, this behaviour will fail. Additionally, secondary calculations can be used for multiple metrics but each secondary calculation will be applied against each metric and returned in a field that matches the following pattern: `metric_name_secondary_calculation_alias`.


## Where Clauses
Sometimes you'll want to see the metric in the context of a particular filter but this filter isn't neccesarily part of the metric definition. In this case, you can use the `where` parameter of the metrics package. It takes a list of `sql` statements and adds them in as filters to the final CTE in the produced SQL. 

Additionally, this parameter can be used by BI Tools to as a way for filters in their UI to be passed through into the metric logic.

## Calendar 
The package comes with a [basic calendar table](/models/dbt_metrics_default_calendar.sql), running between 2010-01-01 and 2029-12-31 inclusive. You can replace it with any custom calendar table which meets the following requirements:
- Contains a `date_day` column. 
- Contains the following columns: `date_week`, `date_month`, `date_quarter`, `date_year`, or equivalents. 
- Additional date columns need to be prefixed with `date_`, e.g. `date_4_5_4_month` for a 4-5-4 retail calendar date set. Dimensions can have any name (see [dimensions on calendar tables](#dimensions-on-calendar-tables)).

To do this, set the value of the `dbt_metrics_calendar_model` variable in your `dbt_project.yml` file: 
```yaml
#dbt_project.yml
config-version: 2
[...]
vars:
    dbt_metrics_calendar_model: my_custom_calendar
```

### Dimensions from calendar tables
You may want to aggregate metrics by a dimension in your custom calendar table, for example `is_weekend`. You can include this within the list of `dimensions` in the macro call **without** it needing to be defined in the metric definition. 

To do so, set a list variable at the project level called `custom_calendar_dimension_list`, as shown in the example below.

```yml
vars:
  custom_calendar_dimension_list: ["is_weekend"]
```

The `is_weekend` field can now be used by your metrics. 

## Time Grains 
The package protects against nonsensical secondary calculations, such as a month-to-date aggregate of data which has been rolled up to the quarter. If you customise your calendar (for example by adding a [4-5-4 retail calendar](https://calogica.com/sql/dbt/2018/11/15/retail-calendar-in-sql.html) month), you will need to override the [`get_grain_order()`](/macros/secondary_calculations/validate_grain_order.sql) macro. In that case, you might remove `month` and replace it with `month_4_5_4`. All date columns must be prefixed with `date_` in the table. Do not include the prefix when defining your metric, it will be added automatically.

## Custom aggregations 
To create a custom primary aggregation (as exposed through the `type` config of a metric), create a macro of the form `metric_my_aggregate(expression)`, then override the [`gen_primary_metric_aggregate()`](/macros/sql_gen/gen_primary_metric_aggregate.sql) macro to add it to the dispatch list. The package also protects against nonsensical secondary calculations such as an average of an average; you will need to override the [`get_metric_allowlist()`](/macros/secondary_calculations/validate_aggregate_coherence.sql)  macro to both add your new aggregate to to the existing aggregations' allowlists, and to make an allowlist for your new aggregation:
```
    {% do return ({
        "average": ['max', 'min'],
        "count": ['max', 'min', 'average', 'my_new_aggregate'],
        [...]
        "my_new_aggregate": ['max', 'min', 'sum', 'average', 'my_new_aggregate']
    }) %}
```

To create a custom secondary aggregation (as exposed through the `secondary_calculations` parameter in the `metric` macro), create a macro of the form `secondary_calculation_my_calculation(metric_name, dimensions, calc_config)`, then override the [`perform_secondary_calculations()`](/macros/secondary_calculations/perform_secondary_calculation.sql) macro. 

## Secondary calculation column aliases
Aliases can be set for a secondary calculation. If no alias is provided, one will be automatically generated. To modify the existing alias logic, or add support for a custom secondary calculation, override [`generate_secondary_calculation_alias()`](/macros/secondary_calculations/generate_secondary_calculation_alias.sql).

