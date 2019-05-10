import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

QCARCHIVE_LOG = "https://qcarchive.molssi.org/images/QCArchiveLogo.png"

navbar = dbc.NavbarSimple(

    children=[
        dbc.NavItem(dbc.NavLink("Service Status", href="/service")),
        dbc.NavItem(dbc.NavLink("Queue Status", href="/queue")),
        dbc.NavItem(dbc.NavLink("Managers", href="/manager")),
    ],
    brand="QCFractal Dashboard",
    brand_href="/",
    sticky="top",
)

