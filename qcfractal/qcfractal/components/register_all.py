# This file is used in the flask_app and db stuff to register routes
# (ie, this file is imported, which causes all the routes
# to be registered with the blueprint)

from .molecules import db_models, routes
from .auth import db_models, routes
from .serverinfo import db_models, routes
from .managers import db_models, routes
from .tasks import db_models, routes
from .services import db_models
from .internal_jobs import db_models, routes
from .external_files import db_models, routes

from . import record_db_models, dataset_db_models, record_routes, dataset_routes
from .singlepoint import record_db_models, dataset_db_models, routes
from .optimization import record_db_models, dataset_db_models, routes
from .torsiondrive import record_db_models, dataset_db_models, routes
from .gridoptimization import record_db_models, dataset_db_models, routes
from .reaction import record_db_models, dataset_db_models, routes
from .manybody import record_db_models, dataset_db_models, routes
from .neb import record_db_models, dataset_db_models, routes
