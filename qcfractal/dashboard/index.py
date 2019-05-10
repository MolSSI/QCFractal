import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from .app import app
# from .apps import reaction_viewer, app2


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/reaction_viewer':
        return reaction_viewer.layout()
    elif pathname == '/apps/app2':
        return app2.layout
    else:
        return html.Div([dcc.Link('Reaction Viewer', href='/apps/reaction_viewer')])
