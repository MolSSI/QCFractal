from abc import ABC, abstractmethod
from .models import TorsionDriveProcedureORM


class QueryBase(ABC):

    # The name/alias used by the REST APIs to access this class
    _class_name = None

    # Mapping of the requested feature and the internal query method
    _query_method_map = {}


    def query(self, session, query_key, **kwargs):

        if query_key not in self._query_method_map:
            raise TypeError(f'Query type {query_key} is unimplemented for class {self._class_name}')

        self.session = session

        return getattr(self, self._query_method_map[query_key])(**kwargs)

    @staticmethod
    def _raise_missing_attribute(cls, query_key, missing_attribute, amend_msg=''):
        """Raises error for missinfg attribute in a message suitable for the REST user"""

        raise AttributeError(f'To query {cls._class_name} for {query_key} '
                             f'you must provide {missing_attribute}.')


class TorsionDriveQueries(QueryBase):

    _class_name = 'torsiondrive'

    _query_method_map = {
        'initial_molecule' : '_get_initial_molecule'
    }

    def _get_initial_molecule(self, torsion_id=None):

        if torsion_id is None:
            self._raise_missing_attribute('initial_molecule', 'torsion drive id')

        # TODO: check count first for other queries, have defensive LIMIT
        sql_statement = f"""
                Select initial_molecule from optimization_procedure as opt where opt.id in
                (
                    Select opt_id from optimization_history where torsion_id = {torsion_id}
                )  
                order by opt.id
        """

        result = self.session.execute(sql_statement).fetchall()

        return [ele[0] for ele in result]



