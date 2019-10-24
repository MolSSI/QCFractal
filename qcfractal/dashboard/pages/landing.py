import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go

import dash_coreui_components as coreui

from ..connection import get_socket
from ..app import app

## Layout

top_row = dbc.Row([
    dbc.Col(
        coreui.AppCard(
            [
                dbc.CardHeader("Card Title"),
                dbc.CardBody([
                    html.P(
                        "Some quick example text to build on the card title and "
                        "make up the bulk of the card's content.",
                        className="card-text text-white",
                    )
                ]),
            ],
            className="bg-warning",
        ),
        className="col-sm-6 col-md-3",
    ),
    dbc.Col(
        coreui.AppCard(
            [
                dbc.CardHeader("Card Title"),
                dbc.CardBody([
                    html.P(
                        "Some quick example text to build on the card title and "
                        "make up the bulk of the card's content.",
                        className="card-text text-white",
                    )
                ]),
            ],
            className="bg-info",
        ),
        className="col-sm-6 col-md-3",
    ),
    dbc.Col(
        coreui.AppCard(
            [
                dbc.CardHeader("Card Title"),
                dbc.CardBody([
                    html.P(
                        "Some quick example text to build on the card title and "
                        "make up the bulk of the card's content.",
                        className="card-text text-white",
                    )
                ]),
            ],
            className="bg-success",
        ),
        className="col-sm-6 col-md-3",
    ),
    dbc.Col(
        coreui.AppCard(
            [
                dbc.CardHeader("Card Title"),
                dbc.CardBody([
                    html.P(
                        "Some quick example text to build on the card title and "
                        "make up the bulk of the card's content.",
                        className="card-text text-white",
                    )
                ]),
            ],
            className="bg-primary",
        ),
        className="col-sm-6 col-md-3",
    )
])


def managers_table(status):
    socket = get_socket()

    managers = socket.get_managers(status=status)
    df = pd.DataFrame(managers["data"])

    df["uuid"] = [x[:6] for x in df["uuid"]]
    cols = ["cluster", "username", "tag", "submitted", "completed", "failures"]

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
                              value=["ACTIVE"],
                              labelStyle={'display': 'inline-block'})


def overview_graph(status):
    socket = get_socket()

    managers = socket.get_managers(status=status)
    df = pd.DataFrame(managers["data"])

    if df.shape[0] > 0:
        data = df.groupby("cluster")["completed"].sum().sort_values(ascending=False)
        bar_data = {"x": data.index, "y": data.values}
    else:
        bar_data = {"x": [], "y": []}
    return go.Figure(data=[go.Bar(bar_data)], layout=go.Layout(margin={"t": 5, "b": 5}))


managers = dbc.Row([
    dbc.Col(
        [
            coreui.AppCard([
                dbc.CardHeader("Manager Information"),
                dbc.CardBody([
                    dbc.Col([
                        dbc.Row([groupby_items]),
                    ]),
                    #        dbc.Col([html.H2("Current status"), dcc.Graph(id="manager-overview", figure=overview_graph(None))]),
                    dcc.Graph(id="manager-overview", style={"width": "100%"}),
                ])
                #    ]),
                #    html.H2("Raw Manager Statistics"),
                #    mana#gers_table("ACTIVE"),
            ])
        ],
        className="w-100")
], className="w-100")


@app.callback(Output('manager-overview', 'figure'), [Input('manager-overview-groupby', 'value')])
def update_overview_graph(status):
    return overview_graph(status)


### Build page return
layout = dbc.Row([top_row, managers])

navbar = {
    "name": "Dashboard",
    "url": "/",
    "icon": "cui-speedometer icons",
    "badge": {
        "variant": "info",
        #        'text': 'NEW'
    },
}
data = {"layout": layout, "navitem": navbar, "id": "dashcard", "route": "/"}
