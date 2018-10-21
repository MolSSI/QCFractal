import qcfractal
import time

from qcfractal import testing

#def _run_tests()

def test_server_boot():
    with testing.popen(["qcfractal-server", "mydb"], coverage=True, dump_stdout=True) as handle:
        time.sleep(2)

def test_server_fireworks_boot():
    args = ["qcfractal-server", "mydb", "--fireworks-manager"]
    with testing.popen(args, coverage=True, dump_stdout=True) as handle:
        time.sleep(2)

def test_server_dask_boot():
    args = ["qcfractal-server", "mydb", "--dask-manager"]
    with testing.popen(args, coverage=True, dump_stdout=True) as handle:
        time.sleep(5)


if __name__ == "__main__":

    def _run_tests(name, func):
        try:
            func()
            print("Passed! The tests {} failed.".format(name))
        except:
            print("ERROR!  The tests {} failed.".format(name))

    local_snapshot = dict(locals()).items()
    for key, local in local_snapshot:
        if not (key.startswith("test_") and callable(local)):
            continue

        _run_tests(key, local)
