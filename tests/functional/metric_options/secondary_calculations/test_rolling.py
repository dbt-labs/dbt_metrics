from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
    custom_calendar_sql
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
if os.getenv('dbt_target') == 'snowflake':
    rolling_average__expected_csv = """
date_month,rolling_average,rolling_average_rolling_max_2_month,rolling_average_rolling_min_2_month
2022-01-01,1,1,1
2022-02-01,1.333333,1.333333,1
""".lstrip()
else:
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

# models/rolling_count.sql
rolling_count_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_count'), 
    grain='week',
    secondary_calculations=[
        metrics.rolling(aggregate="min"),
        metrics.rolling(aggregate="max"),
        metrics.rolling(aggregate="sum"),
        metrics.rolling(aggregate="average")
    ]
    )
}}
"""

# models/rolling_count.yml
rolling_count_yml = """
version: 2 
models:
  - name: rolling_count
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_count__expected')
metrics:
  - name: rolling_count
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_count__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_count__expected_csv = """
date_week,rolling_count,rolling_count_rolling_min,rolling_count_rolling_max,rolling_count_rolling_sum,rolling_count_rolling_average
2022-01-03,2,2,2,2,2.000
2022-01-10,1,1,2,3,1.500
2022-01-17,3,1,3,6,2.000
2022-01-24,1,1,3,7,1.750
2022-01-31,1,1,3,8,1.600
2022-02-07,1,1,3,9,1.500
2022-02-14,1,1,3,10,1.428
""".lstrip()
else:
    rolling_count__expected_csv = """
date_week,rolling_count,rolling_count_rolling_min,rolling_count_rolling_max,rolling_count_rolling_sum,rolling_count_rolling_average
2022-01-03,2,2,2,2,2.0000000000000000
2022-01-10,1,1,2,3,1.5000000000000000
2022-01-17,3,1,3,6,2.0000000000000000
2022-01-24,1,1,3,7,1.7500000000000000
2022-01-31,1,1,3,8,1.6000000000000000
2022-02-07,1,1,3,9,1.5000000000000000
2022-02-14,1,1,3,10,1.4285714285714286
""".lstrip()

# seeds/rolling_count__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_count__expected_yml = """
version: 2
seeds:
  - name: rolling_count__expected
    config:
      column_types:
        date_week: date
        rolling_count: INT64
        rolling_count_rolling_min: INT64
        rolling_count_rolling_max: INT64
        rolling_count_rolling_sum: INT64
        rolling_count_rolling_average: FLOAT64
""".lstrip()
else: 
    rolling_count__expected_yml = """"""

class TestRollingCount:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "table"},
            "vars":{
                "dbt_metrics_calendar_model": "custom_calendar",
                "custom_calendar_dimension_list": ["is_weekend"]
            }
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
            "rolling_count__expected.csv": rolling_count__expected_csv,
            "rolling_count__expected.yml": rolling_count__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "custom_calendar.sql": custom_calendar_sql,
            "rolling_count.sql": rolling_count_sql,
            "rolling_count.yml": rolling_count_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 2

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 4

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]

# models/rolling_sum.sql
rolling_sum_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_sum'), 
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

# models/rolling_sum.yml
rolling_sum_yml = """
version: 2 
models:
  - name: rolling_sum
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_sum__expected')
metrics:
  - name: rolling_sum
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_sum__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_sum__expected_csv = """
date_month,rolling_sum,rolling_sum_rolling_min_2_month,rolling_sum_rolling_max_2_month,rolling_sum_rolling_sum_2_month,rolling_sum_rolling_average_2_month
2022-01-01,18,18,18,18,18.000000
2022-02-01,6,6,18,24,12.000000
""".lstrip()
else:
    rolling_sum__expected_csv = """
date_month,rolling_sum,rolling_sum_rolling_min_2_month,rolling_sum_rolling_max_2_month,rolling_sum_rolling_sum_2_month,rolling_sum_rolling_average_2_month
2022-01-01,18,18,18,18,18.0000000000000000
2022-02-01,6,6,18,24,12.0000000000000000
""".lstrip()

# seeds/rolling_sum__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_sum__expected_yml = """
version: 2
seeds:
  - name: rolling_sum__expected
    config:
      column_types:
        date_month: date
        rolling_sum: INT64
        rolling_sum_rolling_min_2_month: INT64
        rolling_sum_rolling_max_2_month: INT64
        rolling_sum_rolling_sum_2_month: INT64
        rolling_sum_rolling_average_2_month: FLOAT64
""".lstrip()
else: 
    rolling_sum__expected_yml = """"""

class TestRollingSum:

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
            "rolling_sum__expected.csv": rolling_sum__expected_csv,
            "rolling_sum__expected.yml":rolling_sum__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_sum.sql": rolling_sum_sql,
            "rolling_sum.yml": rolling_sum_yml
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

# models/rolling_min.sql
rolling_min_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_min'), 
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

# models/rolling_min.yml
rolling_min_yml = """
version: 2 
models:
  - name: rolling_min
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_min__expected')
metrics:
  - name: rolling_min
    model: ref('fact_orders')
    label: rolling min
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: min
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_min__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_min__expected_csv = """
date_month,rolling_min,rolling_min_rolling_min_2_month,rolling_min_rolling_max_2_month,rolling_min_rolling_sum_2_month,rolling_min_rolling_average_2_month
2022-01-01,1,1,1,1,1.000000
2022-02-01,1,1,1,2,1.000000
""".lstrip()
else: 
    rolling_min__expected_csv = """
date_month,rolling_min,rolling_min_rolling_min_2_month,rolling_min_rolling_max_2_month,rolling_min_rolling_sum_2_month,rolling_min_rolling_average_2_month
2022-01-01,1,1,1,1,1.0000000000000000
2022-02-01,1,1,1,2,1.0000000000000000
""".lstrip()

# seeds/rolling_min__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_min__expected_yml = """
version: 2
seeds:
  - name: rolling_min__expected
    config:
      column_types:
        date_month: date
        rolling_min: INT64
        rolling_min_rolling_min_2_month: INT64
        rolling_min_rolling_max_2_month: INT64
        rolling_min_rolling_sum_2_month: INT64
        rolling_min_rolling_average_2_month: FLOAT64
""".lstrip()
else: 
    rolling_min__expected_yml = """"""

class TestRollingMin:

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
            "rolling_min__expected.csv": rolling_min__expected_csv,
            "rolling_min__expected.yml": rolling_min__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_min.sql": rolling_min_sql,
            "rolling_min.yml": rolling_min_yml
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
      - metrics.metric_equality:
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
if os.getenv('dbt_target') == 'snowflake':
    rolling_max__expected_csv = """
date_month,rolling_max,rolling_max_rolling_min_2_month,rolling_max_rolling_max_2_month,rolling_max_rolling_sum_2_month,rolling_max_rolling_average_2_month
2022-01-01,2,2,2,2,2.000000
2022-02-01,4,2,4,6,3.000000
""".lstrip()
else:
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

# models/rolling_derived_metric.sql
rolling_derived_metric_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_derived_metric'), 
    grain='month',
    secondary_calculations=[
        metrics.rolling(aggregate="max", interval=2),
        metrics.rolling(aggregate="min", interval=2),
        metrics.rolling(aggregate="sum", interval=2)
    ]
    )
}}
"""

# models/base_sum_metric.yml
base_sum_metric_yml = """
version: 2 
metrics:
  - name: base_sum_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# models/rolling_derived_metric.yml
rolling_derived_metric_yml = """
version: 2 
models:
  - name: rolling_derived_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_derived_metric__expected')
metrics:
  - name: rolling_derived_metric
    label: derived ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_derived_metric__expected.csv
rolling_derived_metric__expected_csv = """
date_month,base_sum_metric,rolling_derived_metric,rolling_derived_metric_rolling_max_2_month,rolling_derived_metric_rolling_min_2_month,rolling_derived_metric_rolling_sum_2_month
2022-01-01,8,9,9,9,9
2022-02-01,6,7,9,7,16
""".lstrip()

# seeds/rolling_derived__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_derived__expected_yml = """
version: 2
seeds:
  - name: rolling_derived__expected
    config:
      column_types:
        date_month: date
        rolling_derived: INT64
        rolling_derived_rolling_min_2_month: INT64
        rolling_derived_rolling_max_2_month: INT64
        rolling_derived_rolling_sum_2_month: INT64
""".lstrip()
else: 
    rolling_derived__expected_yml = """"""

class TestRollingDerivedMetric:

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
            "rolling_derived_metric__expected.csv": rolling_derived_metric__expected_csv,
            "rolling_derived__expected.yml": rolling_derived__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "rolling_derived_metric.yml": rolling_derived_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "rolling_derived_metric.sql": rolling_derived_metric_sql
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

        # # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]

# models/rolling_count.sql
rolling_count_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_count'), 
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

# models/rolling_count.yml
rolling_count_yml = """
version: 2 
models:
  - name: rolling_count
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_count__expected')
metrics:
  - name: rolling_count
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_count__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_count__expected_csv = """
date_month,rolling_count,rolling_count_rolling_min_2_month,rolling_count_rolling_max_2_month,rolling_count_rolling_sum_2_month,rolling_count_rolling_average_2_month
2022-01-01,7,7,7,7,7.000000
2022-02-01,3,3,7,10,5.000000
""".lstrip()
else:
    rolling_count__expected_csv = """
date_month,rolling_count,rolling_count_rolling_min_2_month,rolling_count_rolling_max_2_month,rolling_count_rolling_sum_2_month,rolling_count_rolling_average_2_month
2022-01-01,7,7,7,7,7.0000000000000000
2022-02-01,3,3,7,10,5.0000000000000000
""".lstrip()

# seeds/rolling_count__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_count__expected_yml = """
version: 2
seeds:
  - name: rolling_count__expected
    config:
      column_types:
        date_month: date
        rolling_count: INT64
        rolling_count_rolling_min_2_month: INT64
        rolling_count_rolling_max_2_month: INT64
        rolling_count_rolling_sum_2_month: INT64
        rolling_count_rolling_average_2_month: FLOAT64
""".lstrip()
else: 
    rolling_count__expected_yml = """"""

class TestRollingCount:

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
            "rolling_count__expected.csv": rolling_count__expected_csv,
            "rolling_count__expected.yml": rolling_count__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_count.sql": rolling_count_sql,
            "rolling_count.yml": rolling_count_yml
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

# models/rolling_count_distinct.sql
rolling_count_distinct_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_count_distinct'), 
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

# models/rolling_count_distinct.yml
rolling_count_distinct_yml = """
version: 2 
models:
  - name: rolling_count_distinct
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_count_distinct__expected')
metrics:
  - name: rolling_count_distinct
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count_distinct
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_count_distinct__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_count_distinct__expected_csv = """
date_month,rolling_count_distinct,rolling_count_distinct_rolling_min_2_month,rolling_count_distinct_rolling_max_2_month,rolling_count_distinct_rolling_sum_2_month,rolling_count_distinct_rolling_average_2_month
2022-01-01,5,5,5,5,5.000000
2022-02-01,3,3,5,8,4.000000
""".lstrip()
else:
    rolling_count_distinct__expected_csv = """
date_month,rolling_count_distinct,rolling_count_distinct_rolling_min_2_month,rolling_count_distinct_rolling_max_2_month,rolling_count_distinct_rolling_sum_2_month,rolling_count_distinct_rolling_average_2_month
2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/rolling_count_distinct__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_count_distinct__expected_yml = """
version: 2
seeds:
  - name: rolling_count_distinct__expected
    config:
      column_types:
        date_month: date
        rolling_count_distinct: INT64
        rolling_count_distinct_rolling_min_2_month: INT64
        rolling_count_distinct_rolling_max_2_month: INT64
        rolling_count_distinct_rolling_sum_2_month: INT64
        rolling_count_distinct_rolling_average_2_month: FLOAT64
""".lstrip()
else: 
    rolling_count_distinct__expected_yml = """"""

class TestRollingCountDistinct:

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
            "rolling_count_distinct__expected.csv": rolling_count_distinct__expected_csv,
            "rolling_count_distinct__expected.yml": rolling_count_distinct__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_count_distinct.sql": rolling_count_distinct_sql,
            "rolling_count_distinct.yml": rolling_count_distinct_yml
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