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


def bar_plot(traces: 'List[Series]', title=None, ylabel=None, dtype="bar") -> 'plotly.Figure':
    """Renders a plotly bar plot

    Parameters
    ----------
    traces : List[Series]
        A list of bar plots to show, if more than one series the resulting graph will be grouped.
    title : None, optional
        The title of the graph
    ylabel : None, optional
        The y axis label

    Returns
    -------
    plotly.Figure
        The requested bar plot.
    """

    check_plotly()
    import plotly.graph_objs as go

    data = [go.Bar(x=trace.index, y=trace, name=trace.name) for trace in traces]

    layout = {}
    if title:
        layout["title"] = title
    if ylabel:
        layout["yaxis"] = {"title": ylabel}
    layout = go.Layout(layout)
    figure = go.Figure(data=data, layout=layout)

    return figure


def violin_plot(traces: 'DataFrame', negative: 'DataFrame'=None, title=None, points=False,
                ylabel=None) -> 'plotly.Figure':
    """Renders a plotly violin plot

    Parameters
    ----------
    traces : DataFrame
        Pandas DataFrame of points to plot, will create a violin plot of each column.
    negative : DataFrame, optional
        A comparison violin plot, these columns will present the right hand side.
    title : None, optional
        The title of the graph
    points : None, optional
        Show points or not, this option is not available for comparison violin plots.
    ylabel : None, optional
        The y axis label

    Returns
    -------
    plotly.Figure
        The requested violin plot.
    """
    check_plotly()
    import plotly.graph_objs as go

    data = []
    if negative is not None:

        for trace, side in zip([traces, negative], ["positive", "negative"]):
            p = {"name": trace.name, "type": "violin", "box": {"visible": True}}
            p["y"] = trace.stack()
            p["x"] = trace.stack().reset_index().level_1
            p["side"] = side

            data.append(p)
    else:
        for name, series in traces.items():
            p = {"name": name, "type": "violin", "box": {"visible": True}}
            p["y"] = series

            data.append(p)

    layout = go.Layout({"title": title, "yaxis": {"title": ylabel}})
    figure = go.Figure(data=data, layout=layout)

    return figure
