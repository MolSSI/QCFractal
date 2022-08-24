# This file is used in the flask_app and db stuff to register routes
# (ie, this file is imported, which causes all the routes
# to be registered with the blueprint)

from .molecules import db_models, routes
from .outputstore import db_models
from .wavefunctions import db_models
from .nativefiles import db_models
from .permissions import db_models, routes
from .serverinfo import db_models, routes
from .managers import db_models, routes
from .tasks import db_models, routes
from .services import db_models

from . import record_db_models, dataset_db_models, dataset_routes, record_routes
from .singlepoint import record_db_models, dataset_db_models, dataset_routes, record_routes
from .optimization import record_db_models, dataset_db_models, dataset_routes, record_routes
from .torsiondrive import record_db_models, dataset_db_models, dataset_routes, record_routes
from .gridoptimization import record_db_models, dataset_db_models, dataset_routes, record_routes
from .reaction import record_db_models, dataset_db_models, dataset_routes, record_routes
from .manybody import record_db_models, dataset_db_models, dataset_routes, record_routes
from .neb import record_db_models, dataset_db_models, dataset_routes, record_routes
