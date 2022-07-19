import pytest
from dbt.tests.util import run_dbt


# class must begin with 'Test'
class TestExample:

    def test_build(self, project):
        # There's nothing actually to do, and this should pass
        results = run_dbt(["build"])
