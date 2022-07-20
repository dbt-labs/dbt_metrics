import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.integration_tests.fixtures.project"]