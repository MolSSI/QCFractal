# This file is used in the flask_app and db stuff to register routes
# (ie, this file is imported, which causes all the routes
# to be registered with the blueprint)

from .singlepoint import db_models
from .reaction import db_models
from .optimization import db_models, routes
