import dash

app = dash.Dash(__name__)
app.scripts.config.serve_locally = True
app.css.config.serve_locally = True
app.config.suppress_callback_exceptions = True
