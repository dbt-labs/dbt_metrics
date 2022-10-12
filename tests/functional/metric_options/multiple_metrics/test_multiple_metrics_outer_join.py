from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    events_source_csv,
    event_sql,
    event_yml,
)

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from {{
  metrics.calculate(
    [
      metric('count_fr_events'),
      metric('count_uk_events'),
    ],
    grain='month',
  )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2
models:
  - name: multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics__expected')

metrics:
  - name: count_fr_events
    label: Count number of events in France
    model: ref('event')
    type: count
    sql: id
    timestamp: timestamp_field
    time_grains: [month]

    filters:
      - field: country
        operator: '='
        value: "'FR'"

  - name: count_uk_events
    label: Count number of events in UK
    model: ref('event')
    type: count
    sql: id
    timestamp: timestamp_field
    time_grains: [month]

    filters:
      - field: country
        operator: '='
        value: "'UK'"
"""

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,count_fr_events,count_uk_events
2022-01-01,1,
2022-02-01,,1
""".lstrip()

class TestMultipleMetrics:

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
            "events_source.csv": events_source_csv,
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "event.sql": event_sql,
            "event.yml": event_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
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