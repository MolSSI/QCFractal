"""
Visualization using the plotly library.
"""

# Plotly is an optional library
from importlib.util import find_spec
from typing import Any, Dict, List


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


spec = find_spec("plotly")
if spec is None:
    _plotly_found = False
else:
    _plotly_found = True
del spec, find_spec


def check_plotly():
    """
    Checks if plotly is found and auto inits the offline notebook
    """
    if _plotly_found is False:
        raise ModuleNotFoundError(
            "Plotly is required for this function. Please 'conda install plotly' or 'pip isntall plotly'."
        )

    if _isnotebook():
        import plotly

        plotly.offline.init_notebook_mode(connected=True)


def _configure_return(figure, filename, return_figure):
    import plotly

    if return_figure is None:
        return_figure = not _isnotebook()

    if return_figure:
        return figure
    else:
        return plotly.offline.iplot(figure, filename=filename)


def custom_plot(data: Any, layout: Any, return_figure=True) -> "plotly.Figure":
    """A custom plotly plot where the data and layout are pre-specified

    Parameters
    ----------
    data : Any
        Plotly data block
    layout : Any
        Plotly layout block
    return_figure : bool, optional
        Returns the raw plotly figure or not
    """

    check_plotly()
    import plotly.graph_objs as go

    figure = go.Figure(data=data, layout=layout)

    return _configure_return(figure, "qcportal-bar", return_figure)


def bar_plot(traces: "List[Series]", title=None, ylabel=None, return_figure=True) -> "plotly.Figure":
    """Renders a plotly bar plot

    Parameters
    ----------
    traces : List[Series]
        A list of bar plots to show, if more than one series the resulting graph will be grouped.
    title : None, optional
        The title of the graph
    ylabel : None, optional
        The y axis label
    return_figure : bool, optional
        Returns the raw plotly figure or not

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

    return _configure_return(figure, "qcportal-bar", return_figure)


def violin_plot(
    traces: "DataFrame", negative: "DataFrame" = None, title=None, points=False, ylabel=None, return_figure=True
) -> "plotly.Figure":
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
    return_figure : bool, optional
        Returns the raw plotly figure or not

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

    return _configure_return(figure, "qcportal-violin", return_figure)


def scatter_plot(
    traces: List[Dict[str, Any]],
    mode="lines+markers",
    title=None,
    ylabel=None,
    xlabel=None,
    xline=True,
    yline=True,
    custom_layout=None,
    return_figure=True,
) -> "plotly.Figure":
    """Renders a plotly scatter plot

    Parameters
    ----------
    traces : List[Dict[str, Any]]
        A List of traces to plot, require x and y values
    mode : str, optional
        The mode of lines, will not override mode in the traces dictionary
    title : None, optional
        The title of the graph
    ylabel : None, optional
        The y axis label
    xlabel : None, optional
        The x axis label
    xline : bool, optional
        Show the x-zeroline
    yline : bool, optional
        Show the y-zeroline
    custom_layout : None, optional
        Overrides all other layout options
    return_figure : bool, optional
        Returns the raw plotly figure or not

    Returns
    -------
    plotly.Figure
        The requested scatter plot.

    """
    check_plotly()
    import plotly.graph_objs as go

    data = []
    for trace in traces:
        data.append(go.Scatter(**trace))

    if custom_layout is None:
        layout = go.Layout(
            {
                "title": title,
                "yaxis": {"title": ylabel, "zeroline": yline},
                "xaxis": {"title": xlabel, "zeroline": xline},
            }
        )
    else:
        layout = go.Layout(**custom_layout)
    figure = go.Figure(data=data, layout=layout)

    return _configure_return(figure, "qcportal-violin", return_figure)
