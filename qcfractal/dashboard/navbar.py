import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Managers", href="/manager")),
        dbc.NavItem(dbc.NavLink("Link", href="#")),
    ],
    brand="QCFractal Dashboard",
    brand_href="#",
    sticky="top",
)
