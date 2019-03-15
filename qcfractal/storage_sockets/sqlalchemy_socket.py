"""
SQLAlchemy Database class to handle access to Pstgres through ORM
"""


try:
    import sqlalchemy
except ImportError:
    raise ImportError(
        "SQLAlchemy_socket requires sqlalchemy, please install this python module or try a different db_socket.")



from sqlalchemy import Integer, Column, String, ForeignKey, Float
from sqlalchemy import create_engine
from .sql_models import Base
from sqlalchemy.orm import sessionmaker, relationship
from contextlib import contextmanager



import logging
import secrets
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Union



from .sql_models import (CollectionORM, KeywordsORM, KVStoreORM, MoleculeORM, ProcedureORM, QueueManagerORM, ResultORM,
                        ServiceQueueORM, TaskQueueORM, UserORM)
from .storage_utils import add_metadata_template, get_metadata_template
from ..interface.models import KeywordSet, Molecule, ResultRecord, prepare_basis


_null_keys = {"basis", "keywords"}
_id_keys = {"id", "molecule", "keywords", "procedure_id"}
_lower_func = lambda x: x.lower()
_prepare_keys = {"program": _lower_func, "basis": prepare_basis, "method": _lower_func, "procedure": _lower_func}


class SQLAlcehmySocket:
    """
        SQLAlcehmy QCDB wrapper class.
    """

    def __init__(self,
                 uri: str,
                 project: str="molssidb",
                 bypass_security: bool=False,
                 allow_read: bool=True,
                 logger: 'Logger'=None,
                 max_limit: int=1000):
        """
        Constructs a new SQLAlchemy socket

        """

        # Logging data
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('SQLAlcehmySocket')

        # Security
        self._bypass_security = bypass_security
        self._allow_read = allow_read

        self._lower_results_index = ["method", "basis", "program"]

        # disconnect from any active default connection
        # disconnect()

        # Connect to DB and create session

        # echo for logging into python logging
        engine = create_engine(uri, echo=True)

        self.Session = sessionmaker(bind=engine)

        # actually create the tables
        Base.metadata.create_all(engine)

        # if expanded_uri["password"] is not None:
        #     # connect to mongoengine
        #     self.client = db.connect(db=project, host=uri, authMechanism=authMechanism, authSource=authSource)
        # else:
        #     # connect to mongoengine
        #     self.client = db.connect(db=project, host=uri)

        # self._url, self._port = expanded_uri["nodelist"][0]

        # try:
        #     version_array = self.client.server_info()['versionArray']
        #
        #     if tuple(version_array) < (3, 2):
        #         raise RuntimeError
        # except AttributeError:
        #     raise RuntimeError(
        #         "Could not detect MongoDB version at URL {}. It may be a very old version or installed incorrectly. "
        #         "Choosing to stop instead of assuming version is at least 3.2.".format(uri))
        # except RuntimeError:
        #     # Trap low version
        #     raise RuntimeError("Connected MongoDB at URL {} needs to be at least version 3.2, found version {}.".
        #                        format(uri, self.client.server_info()['version']))

        self._project_name = project
        self._max_limit = max_limit


    def __str__(self) -> str:
        return "<SQLAlchemy: address='{0:s}:{1:d}:{2:s}'>".format(str(self._url), self._port, str(self._project_name))

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope"""

        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def _clear_db(self, db_name: str):
        """Dangerous, make sure you are deleting the right DB"""

        self.logger.warning("Clearing database '{}' and dropping all tables.".format(db_name))

        with self.session_scope() as session:
            session.query(ResultORM).delete()
            session.query(MoleculeORM).delete()
            session.query(KeywordsORM).delete()
            session.query(KVStoreORM).delete()
            session.query(CollectionORM).delete()
            session.query(TaskQueueORM).delete()
            session.query(ServiceQueueORM).delete()
            session.query(QueueManagerORM).delete()
            session.query(ProcedureORM).delete()
            session.query(UserORM).delete()

            # self.client.drop_database(db_name)

    def get_project_name(self) -> str:
        return self._project_name

    def get_limit(self, limit: Optional[int]) -> int:
        """Get the allowed limit on results to return in queries based on the
         given `limit`. If this number is greater than the
         mongoengine_soket.max_limit then the max_limit will be returned instead.
        """

        return limit if limit and limit < self._max_limit else self._max_limit

