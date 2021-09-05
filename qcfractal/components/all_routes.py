# This file is used in the flask_app to register routes
# (ie, this file is imported, which causes all the routes
# to be registered with the blueprint)

from .molecule import routes
from .outputstore import routes
from .wavefunctions import routes
from .keywords import routes
from .permissions import routes
from .serverinfo import routes
from .managers import routes
from .records import routes
from .tasks import routes
from .services import routes
from .datasets import routes
