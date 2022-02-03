# This file is used in the flask_app and db stuff to register routes
# (ie, this file is imported, which causes all the routes
# to be registered with the blueprint)

from .molecules import db_models, routes
from .outputstore import db_models
from .wavefunctions import db_models
from .keywords import db_models, routes
from .permissions import db_models, routes
from .serverinfo import db_models, routes
from .managers import db_models, routes
from .tasks import db_models, routes
from .services import db_models
from .records import db_models, routes
from .records import register_all
from .datasets import db_models, routes
from .datasets import register_all
