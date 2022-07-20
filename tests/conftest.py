import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'postgres',
        'threads': 1,
        'host': os.getenv('POSTGRES_TEST_HOST'),
        'user': os.getenv('POSTGRES_TEST_USER'),
        'password': os.getenv('POSTGRES_TEST_PASS'),
        'port': int(os.getenv('POSTGRES_TEST_PORT')),
        'database': os.getenv('POSTGRES_TEST_DBNAME'),
    }