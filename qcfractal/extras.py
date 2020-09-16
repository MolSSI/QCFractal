"""
Misc information and runtime information.
"""

from importlib.util import find_spec

from . import _version

__all__ = ["get_information"]

versions = _version.get_versions()

__info = {"version": versions["version"], "git_revision": versions["full-revisionid"]}


def _isnotebook():
    """
    Checks if we are inside a jupyter notebook or not.
    """
    try:
        shell = get_ipython().__class__.__name__
        if shell in ["ZMQInteractiveShell", "google.colab._shell"]:
            return True
        elif shell == "TerminalInteractiveShell":
            return False
        else:
            return False
    except NameError:
        return False


__info["isnotebook"] = _isnotebook()


def find_module(name):
    return find_spec(name)


def get_information(key):
    """
    Obtains a variety of runtime information about QCFractal.
    """
    key = key.lower()
    if key not in __info:
        raise KeyError("Information key '{}' not understood.".format(key))

    return __info[key]


def provenance_stamp(routine):
    """Return dictionary satisfying QCSchema,
    generating routine's name is passed in through `routine`.

    """
    return {"creator": "QCFractal", "version": get_information("version"), "routine": routine}
