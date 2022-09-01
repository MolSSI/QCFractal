"""
Tests the server compute capabilities.
"""

import multiprocessing
import time

import pytest

from qcfractal.flask_app.flask_app import FlaskProcess
from qcfractal.flask_app.gunicorn_app import GunicornProcess
from qcfractal.periodics import PeriodicsProcess
from qcfractal.process_runner import ProcessRunner
from qcfractal.snowflake import SnowflakeComputeProcess

_test_base_classes = [PeriodicsProcess, FlaskProcess, GunicornProcess, SnowflakeComputeProcess]

pytestmark = pytest.mark.slow


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_quick_stop(stopped_snowflake, test_base_class):
    """
    Tests that quickly starting then stopping a process is ok
    """

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake._flask_proc.start()

    config = stopped_snowflake._qcf_config
    obj = test_base_class(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    runner.stop()
    assert runner.is_alive() is False


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_normal_stop(stopped_snowflake, test_base_class):
    """
    Tests that stopping the process after 5 seconds is ok
    """

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake.start_flask()

    config = stopped_snowflake._qcf_config
    obj = test_base_class(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    time.sleep(5)
    runner.stop()
    assert runner.is_alive() is False
    assert runner.exitcode() == 0


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_stop_in_setup(stopped_snowflake, test_base_class):
    """
    Tests that stopping the process while it is still in setup
    """

    do_stop = multiprocessing.Event()

    class MockClass(test_base_class):
        def setup(self):
            # Run the base class setup, then signal that we are ready to stop
            test_base_class.setup(self)
            do_stop.set()
            time.sleep(5)

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake.start_flask()

    config = stopped_snowflake._qcf_config
    obj = MockClass(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    do_stop.wait()
    time.sleep(1)
    runner.stop()
    assert runner.is_alive() is False
    assert runner.exitcode() == 0


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_stop_in_run(stopped_snowflake, test_base_class):
    """
    Tests that stopping the process while it is in the run step
    """

    do_stop = multiprocessing.Event()

    class MockClass(test_base_class):
        def run(self):
            # Just pause in the run function. No need to call base class run function
            do_stop.set()
            time.sleep(5)

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake.start_flask()

    config = stopped_snowflake._qcf_config
    obj = MockClass(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    do_stop.wait()
    time.sleep(1)
    runner.stop()
    assert runner.is_alive() is False
    assert runner.exitcode() == 0


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_hang_in_setup(stopped_snowflake, test_base_class):
    """
    Tests that stopping the process while it is hanging in setup
    """

    do_stop = multiprocessing.Event()

    class MockClass(test_base_class):
        def setup(self):
            # Hang for a long time in run
            test_base_class.setup(self)
            do_stop.set()
            time.sleep(180)

        def interrupt(self):
            # Also override the interrupt function, which in some cases calls exit
            pass

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake.start_flask()

    config = stopped_snowflake._qcf_config
    obj = MockClass(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    do_stop.wait()
    time.sleep(1)
    runner.stop()
    assert runner.is_alive() is False
    assert runner.exitcode() != 0


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_hang_in_run(stopped_snowflake, test_base_class):
    """
    Tests that stopping the process while it is hanging in run
    """

    do_stop = multiprocessing.Event()

    class MockClass(test_base_class):
        def run(self):
            # Hang for a long time in run
            do_stop.set()
            time.sleep(180)

        def interrupt(self):
            # Also override the interrupt function, which in some cases calls exit
            pass

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake.start_flask()

    config = stopped_snowflake._qcf_config
    obj = MockClass(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    do_stop.wait()
    time.sleep(1)
    runner.stop()
    assert runner.is_alive() is False
    assert runner.exitcode() != 0


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_exception_in_setup(stopped_snowflake, test_base_class):
    """
    Tests that raising an exception during setup() stops the process, returning non-zero exit code
    """

    do_stop = multiprocessing.Event()

    class MockClass(test_base_class):
        def setup(self):
            test_base_class.setup(self)
            do_stop.set()
            raise RuntimeError("Raising an exception (this is expected)")

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake.start_flask()

    config = stopped_snowflake._qcf_config
    obj = MockClass(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    do_stop.wait()
    time.sleep(1)
    assert runner.is_alive() is False
    assert runner.exitcode() != 0


@pytest.mark.parametrize("test_base_class", _test_base_classes)
def test_exception_in_run(stopped_snowflake, test_base_class):
    """
    Tests that raising an exception during run() stops the process, returning non-zero exit code
    """

    do_stop = multiprocessing.Event()

    class MockClass(test_base_class):
        def run(self):
            do_stop.set()
            raise RuntimeError("Raising an exception (this is expected)")

    # Snowflake compute requires running flask
    if test_base_class is SnowflakeComputeProcess:
        stopped_snowflake.start_flask()

    config = stopped_snowflake._qcf_config
    obj = MockClass(config)
    runner = ProcessRunner(f"process_runner_{test_base_class.__name__}", obj, start=False)
    runner.start()
    do_stop.wait()
    time.sleep(1)
    runner.stop()
    assert runner.is_alive() is False
    assert runner.exitcode() != 0
