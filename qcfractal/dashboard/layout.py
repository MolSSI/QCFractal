import dash_bootstrap_components as dbc
import dash_coreui_components as coreui
import dash_html_components as html

from .pages import landing

pages = []
navbar_items = []


def add_page(page_data):
    navbar_items.append(page_data["navitem"])

    cond = coreui.AppRouteConditional(page_data["layout"], id=page_data["id"], route=page_data["route"])
    pages.append(cond)


# Add rows and pages
add_page(landing.data)

# Group1
navbar_items.append({"name": "Components", "title": True})

layout = html.Div(
    [
        coreui.AppHeader(
            [
                coreui.AppNavbarBrand(
                    full={"src": "/assets/images/logo.png", "width": 140, "height": 25, "alt": "QCArchive Logo"}
                ),
                coreui.AppSidebarToggler(id="AppSidebartogglermd", className="d-md-down-none", display="lg"),
            ],
            fixed=True,
        ),
        html.Div(
            [
                coreui.AppSidebar(
                    [
                        coreui.AppSidebarHeader(),
                        coreui.AppSidebarForm(),
                        coreui.AppSidebarNav(id="current-url", navConfig={"items": navbar_items}),
                        coreui.AppSidebarFooter(),
                    ],
                    fixed=True,
                    display="lg",
                ),
                html.Main(
                    [
                        coreui.AppBreadcrumb(appRoutes=[{"path": "/", "name": "Dashboard"}]),
                        dbc.Container(pages, id="page-content", fluid=True),
                    ],
                    className="main",
                ),
            ],
            className="app-body",
        ),
        coreui.AppFooter(),
    ],
    className="app",
)
