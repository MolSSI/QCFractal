import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from .app import app
from .navbar import navbar

from . import dash_managers
from . import dash_queue
from . import dash_service

body = dbc.Container(
    [
        dbc.Row([
            dbc.Col(
                [
                    html.H2("Overview"),
                    html.P("""\
Welcome to the QCFractal Dashboard which will give a high
level overview of the current state of the database.
"""),
                    dbc.Button("View details", color="secondary"),
                ],
                md=4,
            ),
            dbc.Col([
                html.H2("Graph"),
                dcc.Graph(figure={"data": [{
                    "x": [1, 2, 3],
                    "y": [1, 4, 9]
                }]}),
            ]),
        ])
    ],
    className="mt-4",
)

app.layout = html.Div([dcc.Location(id='url', refresh=False), html.Div(id='page-content')])


@app.callback(Output('page-content', 'children'), [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/manager':
        return dash_managers.layout()
    elif pathname == '/queue':
        return dash_queue.layout()
    elif pathname == '/service':
        return dash_service.layout()
    else:
        return html.Div([navbar, body])
