import dash_core_components as dcc
import pandas as pd
from dash.dependencies import Input, Output

import dash_bootstrap_components as dbc
import dash_coreui_components as coreui
from flask import current_app

from ..app import app
from ..connection import get_socket
from ..dash_models import list_managers, manager_graph, task_graph

## Layout

top_row = dbc.Row(
    [
        dbc.Col(coreui.AppCard([dbc.CardHeader("Server Information"), dbc.CardBody(id="fractal-server-information")])),
        dbc.Col(
            coreui.AppCard([dbc.CardHeader("Database Information"), dbc.CardBody(id="fractal-database-information")]),
            # className="col-sm-6 col-md-3",
        ),
        dbc.Col(coreui.AppCard([dbc.CardHeader("Queue Information"), dbc.CardBody(id="fractal-queue-information")])),
    ],
    className="w-100",
)


@app.callback(
    [
        Output("fractal-server-information", "children"),
        Output("fractal-database-information", "children"),
        Output("fractal-queue-information", "children"),
    ],
    [Input("manager-overview-groupby", "value")],
)
def update_server_information(status):
    config = current_app.config["FRACTAL_CONFIG"]
    server = dcc.Markdown(
        f"""
**Name:** {config.fractal.name}

**Query Limit:** {config.fractal.query_limit}
            """
    )
    database = dcc.Markdown(
        f"""
**Name:** {config.database.database_name}

**Port:** {config.database.port}

**Host:** {config.database.host}
        """
    )

    queue = dcc.Graph(figure=task_graph())
    return server, database, queue


def managers_table(status):
    socket = get_socket()

    managers = socket.get_managers(status=status)
    df = pd.DataFrame(managers["data"])

    df["uuid"] = [x[:6] for x in df["uuid"]]
    cols = ["cluster", "username", "tag", "submitted", "completed", "failures"]

    table = dash_table.DataTable(
        id="table",
        columns=[{"name": i.title(), "id": i} for i in cols],
        data=df[cols].to_dict("records"),
        filtering=True,
        sorting=True,
    )
    return table


groupby_items = dcc.Checklist(
    id="manager-overview-groupby",
    options=[{"label": "ACTIVE", "value": "ACTIVE"}, {"label": "INACTIVE", "value": "INACTIVE"}],
    value=["ACTIVE"],
    labelStyle={"display": "inline-block"},
)

groupby_items2 = dcc.Checklist(
    id="manager-overview-groupby2",
    options=[{"label": "ACTIVE", "value": "ACTIVE"}, {"label": "INACTIVE", "value": "INACTIVE"}],
    value=["ACTIVE"],
    labelStyle={"display": "inline-block"},
)

managers = dbc.Row(
    [
        dbc.Col(
            [
                coreui.AppCard(
                    [
                        dbc.CardHeader("Manager Information"),
                        dbc.CardBody(
                            [
                                dbc.Col([dbc.Row([groupby_items])]),
                                dcc.Graph(id="manager-overview", style={"width": "100%"}),
                            ]
                        ),
                    ]
                )
            ],
            className="w-100",
        )
    ],
    className="w-100",
)

manager_list = dbc.Row(
    [
        dbc.Col(
            [
                coreui.AppCard(
                    [
                        dbc.CardHeader("Manager List"),
                        dbc.CardBody([dbc.Col([dbc.Row([groupby_items2])]), dbc.Col(id="manager-list")]),
                    ]
                )
            ],
            className="w-100",
        )
    ],
    className="w-100",
)


@app.callback(Output("manager-overview", "figure"), [Input("manager-overview-groupby", "value")])
def update_overview_graph(status):
    return manager_graph(status=status)


@app.callback(Output("manager-list", "children"), [Input("manager-overview-groupby2", "value")])
def update_overview_list(status):
    return list_managers(status=status)


### Build page return
layout = dbc.Row([top_row, managers, manager_list])

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
