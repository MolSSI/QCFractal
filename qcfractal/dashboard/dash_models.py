import pandas as pd
import plotly.graph_objs as go
from plotly.colors import DEFAULT_PLOTLY_COLORS

import dash_bootstrap_components as dbc

from .connection import get_socket

_default_margin = {"t": 5, "b": 5, "r": 5, "l": 5}


def list_managers(status=None, modified_after=None):
    socket = get_socket()

    managers = socket.get_managers(status=status, modified_after=modified_after)
    cols = [
        "cluster",
        "username",
        "tag",
        "completed",
        "submitted",
        "failures",
        "returned",
        "created_on",
        "modified_on",
        "programs",
        "procedures",
        # "name"
    ]

    df = pd.DataFrame(managers["data"])
    df = df[cols]
    df["programs"] = df["programs"].apply(lambda x: ", ".join(sorted(x)))
    df["procedures"] = df["procedures"].apply(lambda x: ", ".join(sorted(x)))
    df.columns = [x.title() for x in cols]
    df.sort_values("Completed", inplace=True, ascending=False)
    return dbc.Table.from_dataframe(df, style={"width": "100%"})


def manager_graph(status=None, modified_after=None):
    socket = get_socket()

    managers = socket.get_managers(status=status, modified_after=modified_after)
    df = pd.DataFrame(managers["data"])

    bars = []
    if df.shape[0] > 0:
        data = df.groupby("cluster")[["completed", "submitted", "failures"]].sum()
        data["error"] = data["failures"]
        data["running"] = data["submitted"] - data["completed"]

        bar_iter = [
            ("error", DEFAULT_PLOTLY_COLORS[3]),
            ("running", DEFAULT_PLOTLY_COLORS[2]),
            ("completed", DEFAULT_PLOTLY_COLORS[0]),
        ]

        data.sort_values("completed", inplace=True, ascending=False)
        for status, color in bar_iter:
            bars.append(go.Bar(name=status.title(), x=data.index, y=data[status], marker_color=color))

    return go.Figure(
        data=bars,
        layout={
            # "yaxis_type": "log",
            "barmode": "stack",
            "yaxis": {"title": "Tasks"},
            "xaxis": {"title": "Cluster"},
            "margin": _default_margin,
        },
    )


def task_graph():

    socket = get_socket()

    cnts = socket.custom_query("task", "counts")
    df = pd.DataFrame(cnts["data"])

    df.loc[df["tag"].isna(), "tag"] = "None"
    order = df.groupby("tag")["count"].sum().sort_values(ascending=False).index

    bar_iter = [
        ("waiting", DEFAULT_PLOTLY_COLORS[0]),
        ("running", DEFAULT_PLOTLY_COLORS[2]),
        ("error", DEFAULT_PLOTLY_COLORS[3]),
    ]

    bars = []
    for status, color in bar_iter:
        bar_data = []
        for tag in order:
            matches = df[(df["status"] == status) & (df["tag"] == tag)]
            bar_data.append(matches["count"].sum())

        bars.append(go.Bar(name=status.title(), x=order, y=bar_data, marker_color=color))

    fig = go.Figure(
        data=bars,
        layout={
            "barmode": "stack",
            "yaxis_type": "log",
            "yaxis": {"title": "Tasks"},
            "xaxis": {"title": "Tag"},
            "margin": _default_margin,
        },
    )

    return fig
