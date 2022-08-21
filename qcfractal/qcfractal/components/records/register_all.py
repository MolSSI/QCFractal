# This file is used in the flask_app and db stuff to register routes
# (ie, this file is imported, which causes all the routes
# to be registered with the blueprint)

from .singlepoint import db_models, routes
from .optimization import db_models, routes
from .torsiondrive import db_models, routes
from .gridoptimization import db_models, routes
from .reaction import db_models, routes
from .manybody import db_models, routes
from .neb import db_models, routes
