<!--- Uncomment the following headers as-needed for unreleased features
# Unreleased
## New features
## Fixes
## Quality of life
## Under the hood
## Contributors:
--->

# dbt_metrics v0.1.5
## What's Changed
* start_date and end_date casting fix by @fivetran-joemarkiewicz in [#22](https://github.com/dbt-labs/dbt_metrics/pull/22)
* Add more integration tests in [#23](https://github.com/dbt-labs/dbt_metrics/pull/23)

## New Contributors
* @fivetran-joemarkiewicz made their first contribution in [#22](https://github.com/dbt-labs/dbt_metrics/pull/22)

**Full Changelog**: https://github.com/dbt-labs/dbt_metrics/compare/0.1.4...0.1.5

# dbt_metrics v0.1.4
Resolves [#16](https://github.com/dbt-labs/dbt_metrics/issues/16) - period over period calculations were inaccurate when a start date was provided

# dbt_metrics v0.1.3
- Constrains the date spine to the min/max date range of the metric's model to avoid returning the entire 20 year spine unnecessarily.
- Adds start_date and end_date for manual overrides. An earlier start date than the source model's min value will pull through empty spine rows, a later one will exclude source data.

# dbt_metrics v0.1.2
Fixes for incorrect constructor in the `rolling` secondary aggregate

# dbt_metrics v0.1.0
The first release of the dbt_metrics package, which generates queries based on a dbt Core metrics definition.