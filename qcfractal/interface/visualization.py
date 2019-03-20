"""
Visualization using the plotly library.
"""


def _isnotebook():
    """
    Checks if we are inside a jupyter notebook or not.
    """
    try:
        shell = get_ipython().__class__.__name__
        if shell in ['ZMQInteractiveShell', 'google.colab._shell']:
            return True
        elif shell == 'TerminalInteractiveShell':
            return False
        else:
            return False
    except NameError:
        return False


# Plotly is an optional library
from importlib.util import find_spec
spec = find_spec('plotly')
if spec is None:
    _plotly_found = False
else:
    _plotly_found = True
del spec, find_spec

_ipycheck = False


def check_plotly():
    """
    Checks if plotly is found and auto inits the offline notebook
    """
    if _plotly_found is False:
        raise ModuleNotFoundError("Plotly is required for this function. Please ")

    if _ipycheck is False:
        import plotly
        plotly.offline.init_notebook_mode(connected=True)
        _ipycheck = True


import plotly.plotly as py
import plotly.graph_objs as go


def bar():

    check_plotly()

    plotly.offline.plot