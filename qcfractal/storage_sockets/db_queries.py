from abc import ABC, abstractmethod
from .models import TorsionDriveProcedureORM


class QueryBase(ABC):

    # The name/alias used by the REST APIs to access this class
    _class_name = None

    # Mapping of the requested feature and the internal query method
    _query_method_map = {}

    def __init__(self, max_limit=10000):
        self.max_limit = max_limit

    def query(self, session, query_key, **kwargs):

        if query_key not in self._query_method_map:
            raise TypeError(f'Query type {query_key} is unimplemented for class {self._class_name}')

        self.session = session

        return getattr(self, self._query_method_map[query_key])(**kwargs)

    def execute_query(self, sql_statement, with_keys=True):
        """Execute sql statemet, apply limit, and return results as dict if needed"""

        sql_statement += f' LIMIT {self.max_limit}'
        result = self.session.execute(sql_statement)
        keys = result.keys()  # get keys before fetching
        result = result.fetchall()

        # create a list of dict with the keys and values of the results (instead of tuples)
        if with_keys:
            result = [dict(zip(keys, res)) for res in result]

        return result

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

    def _get_initial_molecule_id(self, torsion_id=None):

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


    def _get_initial_molecule(self, torsion_id=None):

        if torsion_id is None:
            self._raise_missing_attribute('initial_molecule', 'torsion drive id')

        # TODO: check count first for other queries, have defensive LIMIT
        # TODO: include opt.id as opt_id, ?
        # TODO: order by opt.id ?
        sql_statement = f"""
                Select molecule.* from molecule
                join optimization_procedure as opt
                on molecule.id = opt.initial_molecule
                where opt.id in
                    (Select opt_id from optimization_history where torsion_id = {torsion_id})  
        """

        result = self.session.execute(sql_statement)
        keys = result.keys()
        result = result.fetchall()

        # create a list of dict with the keys and values of the results (instead of tuples)
        return_results = [dict(zip(keys, res)) for res in result]

        return return_results


