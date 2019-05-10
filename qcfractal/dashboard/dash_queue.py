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


def queue_counts(status):
    socket = get_socket()

    data = socket.get_queue(status=status, limit=1)
    return data["meta"]["n_found"]


body = lambda: dbc.Container([
    dbc.Row([
        dbc.Col([html.H2("WAITING"), html.P(queue_counts("WAITING"))]),
        dbc.Col([html.H2("RUNNING"), html.P(queue_counts("RUNNING"))]),
        dbc.Col([html.H2("ERROR"), html.P(queue_counts("ERROR"))]),
        dbc.Col([html.H2("COMPLETE"), html.P(queue_counts("COMPLETE"))]),
    ])
])

layout = lambda: html.Div([navbar, body()])


