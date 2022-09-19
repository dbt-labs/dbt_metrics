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

# models/rolling_max.sql
rolling_max_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_max'), 
    grain='month',
    secondary_calculations=[
        metrics.rolling(aggregate="min", interval=2),
        metrics.rolling(aggregate="max", interval=2),
        metrics.rolling(aggregate="sum", interval=2),
        metrics.rolling(aggregate="average", interval=2)
    ]
    )
}}
"""

# models/rolling_max.yml
rolling_max_yml = """
version: 2 
models:
  - name: rolling_max
    tests: 
      - dbt_utils.equality:
          compare_model: ref('rolling_max__expected')
metrics:
  - name: rolling_max
    model: ref('fact_orders')
    label: rolling min
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: max
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_max__expected.csv
if os.getenv('dbt_target') == 'postgres':
    rolling_max__expected_csv = """
date_month,rolling_max,rolling_max_rolling_min_2_month,rolling_max_rolling_max_2_month,rolling_max_rolling_sum_2_month,rolling_max_rolling_average_2_month
2022-01-01,2,2,2,2,2.0000000000000000
2022-02-01,4,2,4,6,3.0000000000000000
""".lstrip()

# seeds/rolling_max__expected.csv
if os.getenv('dbt_target') == 'redshift':
    rolling_max__expected_csv = """
date_month,rolling_max,rolling_max_rolling_min_2_month,rolling_max_rolling_max_2_month,rolling_max_rolling_sum_2_month,rolling_max_rolling_average_2_month
2022-01-01,2,2,2,2,2.0000000000000000
2022-02-01,4,2,4,6,3.0000000000000000
""".lstrip()

# seeds/rolling_max__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_max__expected_csv = """
date_month,rolling_max,rolling_max_rolling_min_2_month,rolling_max_rolling_max_2_month,rolling_max_rolling_sum_2_month,rolling_max_rolling_average_2_month
2022-01-01,2,2,2,2,2.000000
2022-02-01,4,2,4,6,3.000000
""".lstrip()

# seeds/rolling_max__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    rolling_max__expected_csv = """
date_month,rolling_max,rolling_max_rolling_min_2_month,rolling_max_rolling_max_2_month,rolling_max_rolling_sum_2_month,rolling_max_rolling_average_2_month
2022-01-01,2,2,2,2,2.0000000000000000
2022-02-01,4,2,4,6,3.0000000000000000
""".lstrip()

# seeds/rolling_max__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_max__expected_yml = """
version: 2
seeds:
  - name: rolling_max__expected
    config:
      column_types:
        date_month: date
        rolling_max: INT64
        rolling_max_rolling_min_2_month: INT64
        rolling_max_rolling_max_2_month: INT64
        rolling_max_rolling_sum_2_month: INT64
        rolling_max_rolling_average_2_month: FLOAT64
""".lstrip()
else: 
    rolling_max__expected_yml = """"""

class TestRollingMax:

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
            "rolling_max__expected.csv": rolling_max__expected_csv,
            "rolling_max__expected.yml": rolling_max__expected_yml,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_max.sql": rolling_max_sql,
            "rolling_max.yml": rolling_max_yml
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