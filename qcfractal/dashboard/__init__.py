try:
    import flask
    import dash
    import dash_core_components
    import dash_html_components
    import dash_bootstrap_components
    import dash_coreui_components
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Could not find all dashboard requirements, please `conda install qcfractal-dashboard -c conda-forge` to use the dashboard."
    )


from .app import app
from .layout import layout

app.layout = layout
