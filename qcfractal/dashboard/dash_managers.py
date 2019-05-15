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


groupby_items = dcc.Checklist(id="manager-overview-groupby",
                              options=[
                                  {
                                      'label': 'ACTIVE',
                                      'value': 'ACTIVE'
                                  },
                                  {
                                      'label': 'INACTIVE',
                                      'value': 'INACTIVE'
                                  },
                              ],
                              values=["ACTIVE"],
                              labelStyle={'display': 'inline-block'})

body = lambda: dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Heading"),
            html.P("Hello!"),
            dbc.Row([groupby_items]),
        ]),
        dbc.Col([html.H2("Current status"), dcc.Graph(id="manager-overview", figure=overview_graph(None))]),
    ]),
    html.H2("Raw Manager Statistics"),
    managers_table("ACTIVE"),
])

layout = lambda: html.Div([navbar, body()])


def overview_graph(status):
    socket = get_socket()

    managers = socket.get_managers(status=status)
    df = pd.DataFrame(managers["data"])

    data = df.groupby("cluster")["completed"].sum().sort_values(ascending=False)
    return {"data": [{"x": data.index, "y": data.values}]}

@app.callback(
    Output('manager-overview', 'figure'),
 [Input('manager-overview-groupby', 'value')])
def update_overview_graph(status):
    return overview_graph(status)