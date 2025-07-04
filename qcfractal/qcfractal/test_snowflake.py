import time

import pytest

from qcfractal.snowflake import FractalSnowflake


@pytest.mark.slow
def test_snowflake_restarting(tmp_path):
    s = FractalSnowflake(start=False, tmpdir_parent=str(tmp_path))

    s._start_api()
    s._start_compute()
    s._start_job_runner()

    time.sleep(5)

    s.stop()

    time.sleep(5)

    assert s._api_proc is None
    assert s._compute_proc is None
    assert s._job_runner_proc is None

    s._start_api()
    s._start_compute()
    s._start_job_runner()

    assert s._api_proc.is_alive()
    assert s._compute_proc.is_alive()
    assert s._job_runner_proc.is_alive()

    c = s.client()
    assert c.ping()
