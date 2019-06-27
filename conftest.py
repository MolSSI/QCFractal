"""
Add various markers for test suite

https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
"""


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--runexamples", action="store_true", default=False, help="run example tests")

def pytest_configure(config):
    import sys
    sys._called_from_test = True

def pytest_unconfigure(config):
    import sys
    del sys._called_from_test
