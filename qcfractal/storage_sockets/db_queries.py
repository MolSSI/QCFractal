from abc import ABC
from qcelemental.util import msgpackext_dumps, msgpackext_loads


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

         # TODO: check count first, way to iterate

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

# ----------------------------------------------------------------------------

class TorsionDriveQueries(QueryBase):

    _class_name = 'torsiondrive'

    _query_method_map = {
        'initial_molecules' : '_get_initial_molecules',
        'initial_molecules_ids' : '_get_initial_molecules_ids',
        'return_results': '_get_return_results'
    }

    def _get_initial_molecules_ids(self, torsion_id=None):

        if torsion_id is None:
            self._raise_missing_attribute('initial_molecules_ids', 'torsion drive id')

        sql_statement = f"""
                select initial_molecule from optimization_procedure as opt where opt.id in
                (
                    select opt_id from optimization_history where torsion_id = {torsion_id}
                )  
                order by opt.id
        """

        return self.execute_query(sql_statement, with_keys=False)


    def _get_initial_molecules(self, torsion_id=None):

        if torsion_id is None:
            self._raise_missing_attribute('initial_molecules', 'torsion drive id')

        # TODO: include opt.id as opt_id, ?
        # TODO: order by opt.id ?
        sql_statement = f"""
                select molecule.* from molecule
                join optimization_procedure as opt
                on molecule.id = opt.initial_molecule
                where opt.id in
                    (select opt_id from optimization_history where torsion_id = {torsion_id})
        """

        return self.execute_query(sql_statement, with_keys=True)


    def _get_return_results(self, torsion_id=None):

        if torsion_id is None:
            self._raise_missing_attribute('return_results', 'torsion drive id')

        sql_statement = f"""
                select result.id as result_id, result.return_result from result
                join opt_result_association as opt_res
                on result.id = opt_res.result_id
                where opt_res.opt_id in 
                (
                    select opt_id from optimization_history where torsion_id = {torsion_id}
                )
        """

        return self.execute_query(sql_statement, with_keys=False)