from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
)

# models/rolling_average.sql
rolling_average_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_average'), 
    grain='month',
    secondary_calculations=[
        metrics.rolling(aggregate="max", interval=2),
        metrics.rolling(aggregate="min", interval=2)
    ]
    )
}}
"""

# models/rolling_average.yml
rolling_average_yml = """
version: 2 
models:
  - name: rolling_average
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_average__expected')
metrics:
  - name: rolling_average
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_average__expected.csv
if os.getenv('dbt_target') == 'postgres':
    rolling_average__expected_csv = """
date_month,rolling_average,rolling_average_rolling_max_2_month,rolling_average_rolling_min_2_month
2022-01-01,1,1,1
2022-02-01,1.3333333333333333,1.3333333333333333,1
""".lstrip()

# seeds/rolling_average__expected.csv
if os.getenv('dbt_target') == 'redshift':
    rolling_average__expected_csv = """
date_month,rolling_average,rolling_average_rolling_max_2_month,rolling_average_rolling_min_2_month
2022-01-01,1,1,1
2022-02-01,1.3333333333333333,1.3333333333333333,1
""".lstrip()

# seeds/rolling_average__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_average__expected_csv = """
date_month,rolling_average,rolling_average_rolling_max_2_month,rolling_average_rolling_min_2_month
2022-01-01,1,1,1
2022-02-01,1.333333,1.333333,1
""".lstrip()

# seeds/rolling_average__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    rolling_average__expected_csv = """
date_month,rolling_average,rolling_average_rolling_max_2_month,rolling_average_rolling_min_2_month
2022-01-01,1,1,1
2022-02-01,1.3333333333333333,1.3333333333333333,1
""".lstrip()

# seeds/rolling_average__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_average__expected_yml = """
version: 2
seeds:
  - name: rolling_average__expected
    config:
      column_types:
        date_month: date
        rolling_average: FLOAT64
        rolling_average_rolling_max_2_month: FLOAT64
        rolling_average_rolling_min_2_month: INT64
""".lstrip()
else: 
    rolling_average__expected_yml = """"""

class TestRollingAverage:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "table"}
            }
    else: 
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "view"}
            }  

    # install current repo as package
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"local": os.getcwd()}
                ]
        }


    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "fact_orders_source.csv": fact_orders_source_csv,
            "rolling_average__expected.csv": rolling_average__expected_csv,
            "rolling_average__expected.yml": rolling_average__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_average.sql": rolling_average_sql,
            "rolling_average.yml": rolling_average_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 2

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 3

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]