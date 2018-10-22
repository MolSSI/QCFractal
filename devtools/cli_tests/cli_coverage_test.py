import qcfractal
import time

from qcfractal import testing

#def _run_tests()
_options = {"coverage": True, "dump_stdout": False}

def test_server_boot():
    with testing.popen(["qcfractal-server", "mydb"], **_options) as handle:
        time.sleep(2)

def test_server_fireworks_boot():
    args = ["qcfractal-server", "mydb", "--fireworks-manager"]
    with testing.popen(args, **_options) as handle:
        time.sleep(2)

def test_server_dask_boot():
    args = ["qcfractal-server", "mydb", "--dask-manager"]
    with testing.popen(args, **_options) as handle:
        time.sleep(5)


if __name__ == "__main__":

    def _run_tests(name, func):
        try:
            func()
            print("{:40s} Passed!".format(name))
        except:
            print("{:40s} **FAILED!**.".format(name))

    local_snapshot = dict(locals()).items()
    for key, local in local_snapshot:
        if not (key.startswith("test_") and callable(local)):
            continue

        _run_tests(key, local)
