import dash_table
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from .app import app
from .connection import get_socket
from .navbar import navbar

## Functions to call on the fly when page loads
import pandas as pd


def managers_table(status):
    socket = get_socket()

    managers = socket.get_managers(status=status)
    df = pd.DataFrame(managers["data"])

    df["uuid"] = [x[:6] for x in df["uuid"]]
    cols = ["cluster", "username", "uuid", "submitted", "completed", "failures"]

    table = dash_table.DataTable(
        id='table',
        columns=[{
            "name": i.title(),
            "id": i
        } for i in cols],
        data=df[cols].to_dict('records'),
        filtering=True,
        sorting=True,
    )
    return table


body = lambda: dbc.Container([

    dbc.Row([
        dbc.Col([
            html.H2("Heading"),
            html.P("Hello!")]
        )]),
    html.H2("Raw Manager Statistics"),
    managers_table("ACTIVE"),
    ])

layout = lambda: html.Div([navbar, body()])


@app.callback([
    Output('rds-display-value', 'children'),
    Output('rds-available-methods', 'options'),
    Output('rds-available-basis', 'options')
], [Input('available-rds', 'value')])
def display_value(value):
    display_value = 'You have selected "{}"'.format(value)

    return display_value, get_history_values(value, "method"), get_history_values(value, "basis")


@app.callback(Output('primary-graph', 'figure'), [
    Input('available-rds', 'value'),
    Input('rds-available-methods', 'value'),
    Input('rds-available-basis', 'value'),
    Input('rds-groupby', 'value'),
    Input('rds-metric', 'value'),
    Input('rds-kind', 'value'),
])
def build_graph(dataset, method, basis, groupby, metric, kind):

    client = get_client()

    ds = client.get_collection("reactiondataset", dataset)
    history = ds.list_history(method=method, basis=basis)
    if (method is None) or (basis is None):
        print("")
        return {}

    fig = ds.visualize(method=method, basis=basis, groupby=groupby, metric=metric, kind=kind, return_figure=True)
    return fig
