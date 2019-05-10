import dash
import dash-bootstrap-components as dbc

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
app.config.suppress_callback_exceptions = True
app.server.config.from_mapping(QCPORTAL_URI=None, QCPORTAL_VERIFY=True)

