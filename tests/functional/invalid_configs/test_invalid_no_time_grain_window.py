from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import YamlParseDictError

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml
)

# models/invalid_window_no_time_grain.yml
invalid_window_no_time_grain_yml = """
version: 2 

metrics:
  - name: invalid_window_no_time_grain
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14
        period: day
    dimensions:
      - had_discount
      - order_country
"""

# models/invalid_window_no_time_grain.sql
invalid_window_no_time_grain_sql = """
select *
from 
{{ metrics.calculate(metric('invalid_window_no_time_grain'))
}}
"""

class TestInvalidWindowNoTimeGrain:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
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
            "fact_orders_source.csv": fact_orders_source_csv
            }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "invalid_window_no_time_grain.yml": invalid_window_no_time_grain_yml,
            "invalid_window_no_time_grain.sql": invalid_window_no_time_grain_sql
        }

    def test_failing_window_no_time_grain(self,project):
        with pytest.raises(YamlParseDictError):
            run_dbt(["deps"])
            run_dbt(["run"])


# models/invalid_develop_window_no_time_grain.sql
invalid_develop_window_no_time_grain_sql = """
{% set my_metric_yml -%}

metrics:
  - name: invalid_window_no_time_grain
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14
        period: day
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ metrics.develop(
        metric_list=['invalid_window_no_time_grain'],
        develop_yml=my_metric_yml
        )
    }}
"""

class TestInvalidDevelopWindowAllTime:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
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
            "fact_orders_source.csv": fact_orders_source_csv
            }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "invalid_develop_window_no_time_grain.sql": invalid_develop_window_no_time_grain_sql
        }

    def test_failing_develop_window_no_time_grain(self,project):
        run_dbt(["deps"])
        run_dbt(["seed"])
        run_dbt(["run"], expect_pass = False)
