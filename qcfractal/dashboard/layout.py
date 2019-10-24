import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import dash_coreui_components as coreui

layout = html.Div([
    coreui.AppHeader([
        coreui.AppNavbarBrand(full={
            'src': '/assets/images/logo.svg',
            'width': 89,
            'height': 25,
            'alt': 'CoreUI Logo'
        },
                              minimized={
                                  'src': '/assets/images/sygnet.svg',
                                  'width': 30,
                                  'height': 30,
                                  'alt': 'CoreUI Logo'
                              }),
        coreui.AppSidebarToggler(id='AppSidebartogglermd', className='d-md-down-none', display='lg'),
    ],
                     fixed=True),
    html.Div([
        coreui.AppSidebar([
            coreui.AppSidebarHeader(),
            coreui.AppSidebarForm(),
            coreui.AppSidebarNav(
                id='current-url',
                navConfig={
                    'items': [{
                        'name': 'Dashboard',
                        'url': '/',
                        'icon': 'cui-speedometer icons',
                        'badge': {
                            'variant': 'info',
                            'text': 'NEW'
                        }
                    }, {
                        'name': 'Components',
                        'title': True
                    }, {
                        'name': 'Base',
                        'url': '/base',
                        'icon': 'cui-puzzle icons',
                        'children': [{
                            'name': 'Card',
                            'url': '/base/card',
                            'icon': 'cui-puzzle icons'
                        }]
                    }, {
                        'name': 'Buttons',
                        'url': '/buttons',
                        'icon': 'cui-arrow-left icons'
                    }, {
                        'name': 'Charts',
                        'url': '/charts',
                        'icon': 'cui-chart icons'
                    }, {
                        'name': 'Editors',
                        'url': '/editors',
                        'icon': 'cui-brush icons'
                    }, {
                        'name': 'Forms',
                        'url': '/forms',
                        'icon': 'cui-ban icons'
                    }, {
                        'name': 'Tables',
                        'url': '/tables',
                        'icon': 'cui-ban icons'
                    }, {
                        'name':
                        'Other',
                        'url':
                        '/other',
                        'icon':
                        'cui-star icons',
                        'children': [{
                            'name': 'Animals',
                            'url': '/other/animals',
                            'icon': 'cui-star icons',
                            'badge': {
                                'variant': 'success',
                                'text': 'RAD'
                            }
                        }]
                    }, {
                        'name': 'Group Title',
                        'title': True
                    }, {
                        'name': 'Disabled',
                        'url': '/',
                        'icon': 'cui-ban icons',
                        'attributes': {
                            'disabled': True
                        },
                    }]
                }),
            coreui.AppSidebarFooter()
        ],
                          fixed=True,
                          display='lg'),
        html.Main([
            coreui.AppBreadcrumb(appRoutes=[{
                'path': '/',
                'name': 'Dashboard'
            }]),
            dbc.Container([
                coreui.AppRouteConditional([
                    dbc.Row([
                        dbc.Col(coreui.AppCard([
                            dbc.CardHeader("Card Title"),
                            dbc.CardBody([
                                html.P(
                                    "Some quick example text to build on the card title and "
                                    "make up the bulk of the card's content.",
                                    className="card-text text-white",
                                ),
                            ])
                        ],
                                               style={"width": "18rem"},
                                               className="bg-warning"),
                                className="col-sm-6 col-md-3"),
                        dbc.Col(coreui.AppCard([
                            dbc.CardHeader("Card Title"),
                            dbc.CardBody([
                                html.P(
                                    "Some quick example text to build on the card title and "
                                    "make up the bulk of the card's content.",
                                    className="card-text text-white",
                                ),
                            ]),
                        ],
                                               style={'width': '18rem'},
                                               className="bg-info"),
                                className="col-sm-6 col-md-3"),
                        dbc.Col(coreui.AppCard([
                            dbc.CardHeader("Card Title"),
                            dbc.CardBody([
                                html.P(
                                    "Some quick example text to build on the card title and "
                                    "make up the bulk of the card's content.",
                                    className="card-text text-white",
                                ),
                            ]),
                        ],
                                               style={'width': '18rem'},
                                               className="bg-success"),
                                className="col-sm-6 col-md-3"),
                        dbc.Col(coreui.AppCard([
                            dbc.CardHeader("Card Title"),
                            dbc.CardBody([
                                html.P(
                                    "Some quick example text to build on the card title and "
                                    "make up the bulk of the card's content.",
                                    className="card-text text-white",
                                ),
                            ])
                        ],
                                               style={'width': '18rem'},
                                               className="bg-primary"),
                                className="col-sm-6 col-md-3")
                    ]),
                ],
                                           id='dashcard',
                                           route='/'),
            ],
                          id='page-content',
                          fluid=True)
        ],
                  className='main')
    ],
             className='app-body'),
    coreui.AppFooter()
],
                      className='app')
