try:
    import flask
    import dash
    import dash_core_components
    import dash_html_components
except ModuleNotFound:
    raise ModuleNotFound("Could not find `dash`, please `conda install dash -c conda-forge` to use the dashboard.")

try:
    import dash_bootstrap_components
except ModuleNotFound:
    raise ModuleNotFound(
        "Could not find `dash-bootstrap-components`, please `conda install dash-bootstrap-components -c conda-forge` to use the dashboard."
    )

try:
    import dash_coreui_components
except ModuleNotFound:
    raise ModuleNotFound(
        "Could not find `dash-coreui-components`, please `conda install dash-coreui-components -c conda-forge` to use the dashboard."
    )

from .app import app
from .layout import layout

app.layout = layout
