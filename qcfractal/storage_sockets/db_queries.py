from abc import ABC
from typing import List, Union

from sqlalchemy import Integer, inspect
from sqlalchemy.sql import bindparam, text

from qcfractal.interface.models import Molecule, ResultRecord
from qcfractal.storage_sockets.models import MoleculeORM, ResultORM


class QueryBase(ABC):

    # The name/alias used by the REST APIs to access this class
    _class_name = None

    # Mapping of the requested feature and the internal query method
    _query_method_map = {}

    def __init__(self, max_limit=1000):
        self.max_limit = max_limit

    def query(self, session, query_key, limit=0, skip=0, projection=None, **kwargs):

        if query_key not in self._query_method_map:
            raise TypeError(f'Query type {query_key} is unimplemented for class {self._class_name}')

        self.session = session

        return getattr(self, self._query_method_map[query_key])(**kwargs)

    def execute_query(self, sql_statement, with_keys=True, **kwargs):
        """Execute sql statemet, apply limit, and return results as dict if needed"""

        # TODO: check count first, way to iterate

        # sql_statement += f' LIMIT {self.max_limit}'
        result = self.session.execute(sql_statement, kwargs)
        keys = result.keys()  # get keys before fetching
        result = result.fetchall()
        self.session.commit()

        # create a list of dict with the keys and values of the results (instead of tuples)
        if with_keys:
            result = [dict(zip(keys, res)) for res in result]

        return result

    @staticmethod
    def _raise_missing_attribute(cls, query_key, missing_attribute, amend_msg=''):
        """Raises error for missinfg attribute in a message suitable for the REST user"""

        raise AttributeError(f'To query {cls._class_name} for {query_key} ' f'you must provide {missing_attribute}.')


# ----------------------------------------------------------------------------


class TorsionDriveQueries(QueryBase):

    _class_name = 'torsiondrive'

    _query_method_map = {
        'initial_molecules': '_get_initial_molecules',
        'initial_molecules_ids': '_get_initial_molecules_ids',
        'final_molecules': '_get_final_molecules',
        'final_molecules_ids': '_get_final_molecules_ids',
        'return_results': '_get_return_results',
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

        sql_statement = f"""
                select molecule.* from molecule
                join optimization_procedure as opt
                on molecule.id = opt.initial_molecule
                where opt.id in
                    (select opt_id from optimization_history where torsion_id = {torsion_id})
        """

        return self.execute_query(sql_statement, with_keys=True)

    def _get_final_molecules_ids(self, torsion_id=None):

        if torsion_id is None:
            self._raise_missing_attribute('final_molecules_ids', 'torsion drive id')

        sql_statement = f"""
                select final_molecule from optimization_procedure as opt where opt.id in
                (
                    select opt_id from optimization_history where torsion_id = {torsion_id}
                )
                order by opt.id
        """

        return self.execute_query(sql_statement, with_keys=False)

    def _get_final_molecules(self, torsion_id=None):

        if torsion_id is None:
            self._raise_missing_attribute('final_molecules', 'torsion drive id')

        sql_statement = f"""
                select molecule.* from molecule
                join optimization_procedure as opt
                on molecule.id = opt.final_molecule
                where opt.id in
                    (select opt_id from optimization_history where torsion_id = {torsion_id})
        """

        return self.execute_query(sql_statement, with_keys=True)

    def _get_return_results(self, torsion_id=None):
        """All return results ids of a torsion drive"""

        if torsion_id is None:
            self._raise_missing_attribute('return_results', 'torsion drive id')

        sql_statement = f"""
                select opt_res.opt_id, result.id as result_id, result.return_result from result
                join opt_result_association as opt_res
                on result.id = opt_res.result_id
                where opt_res.opt_id in
                (
                    select opt_id from optimization_history where torsion_id = {torsion_id}
                )
        """

        return self.execute_query(sql_statement, with_keys=False)


class OptimizationQueries(QueryBase):

    _class_name = 'optimization'
    _exclude = ['molecule_hash', 'molecular_formula', 'result_type']
    _query_method_map = {
        'all_results': '_get_all_results',
        'final_result': '_get_final_results',
        'initial_molecule': '_get_initial_molecules',
        'final_molecule': '_get_final_molecules',
    }

    def _remove_excluded_keys(self, data):
        for key in self._exclude:
            data.pop(key, None)

    def _get_all_results(self, optimization_ids: List[Union[int, str]] = None):
        """Returns all the results objects (trajectory) of each optmization
        Returns list(list) """

        if optimization_ids is None:
            self._raise_missing_attribute('all_results', 'List of optimizations ids')

        # row_to_json(result.*)
        sql_statement = text("""
            select * from base_result
            join (
                select opt_id, result.* from result
                join opt_result_association as traj
                on result.id = traj.result_id
                where traj.opt_id in :optimization_ids
            ) result
            on base_result.id = result.id
        """)

        # bind and expand ids list
        sql_statement = sql_statement.bindparams(bindparam("optimization_ids", expanding=True))

        # column types:
        columns = inspect(ResultORM).columns
        sql_statement = sql_statement.columns(opt_id=Integer, *columns)
        query_result = self.execute_query(sql_statement, optimization_ids=list(optimization_ids))

        ret = {}
        for rec in query_result:
            self._remove_excluded_keys(rec)
            key = rec.pop('opt_id')
            if key not in ret:
                ret[key] = []

            ret[key].append(ResultRecord(**rec))

        return ret

    def _get_final_results(self, optimization_ids: List[Union[int, str]] = None):
        """Return the actual results objects of the best result in each optimization"""

        if optimization_ids is None:
            self._raise_missing_attribute('final_result', 'List of optimizations ids')

        sql_statement = text("""
            select * from base_result
            join (
                select opt_id, result.* from result
                join (
                    select opt.opt_id, opt.result_id, max_pos from opt_result_association as opt
                    inner join (
                            select opt_id, max(position) as max_pos from opt_result_association
                            where opt_id in :optimization_ids
                            group by opt_id
                        ) opt2
                    on opt.opt_id = opt2.opt_id and opt.position = opt2.max_pos
                ) traj
                on result.id = traj.result_id
            ) result
            on base_result.id = result.id
        """)

        # bind and expand ids list
        sql_statement = sql_statement.bindparams(bindparam("optimization_ids", expanding=True))

        # column types:
        columns = inspect(ResultORM).columns
        sql_statement = sql_statement.columns(opt_id=Integer, *columns)
        query_result = self.execute_query(sql_statement, optimization_ids=list(optimization_ids))

        ret = {}
        for rec in query_result:
            self._remove_excluded_keys(rec)
            key = rec.pop('opt_id')
            ret[key] = ResultRecord(**rec)

        return ret

    def _get_initial_molecules(self, optimization_ids=None):

        if optimization_ids is None:
            self._raise_missing_attribute('initial_molecule', 'List of optimizations ids')

        sql_statement = text("""
                select opt.id as opt_id, molecule.* from molecule
                join optimization_procedure as opt
                on molecule.id = opt.initial_molecule
                where opt.id in :optimization_ids
        """)

        # bind and expand ids list
        sql_statement = sql_statement.bindparams(bindparam("optimization_ids", expanding=True))

        # column types:
        columns = inspect(MoleculeORM).columns
        sql_statement = sql_statement.columns(opt_id=Integer, *columns)
        query_result = self.execute_query(sql_statement, optimization_ids=list(optimization_ids))

        ret = {}
        for rec in query_result:
            self._remove_excluded_keys(rec)
            key = rec.pop('opt_id')
            ret[key] = Molecule(**rec)

        return ret

    def _get_final_molecules(self, optimization_ids=None):

        if optimization_ids is None:
            self._raise_missing_attribute('final_molecule', 'List of optimizations ids')

        sql_statement = text("""
                select opt.id as opt_id, molecule.* from molecule
                join optimization_procedure as opt
                on molecule.id = opt.final_molecule
                where opt.id in :optimization_ids
        """)

        # bind and expand ids list
        sql_statement = sql_statement.bindparams(bindparam("optimization_ids", expanding=True))

        # column types:
        columns = inspect(MoleculeORM).columns
        sql_statement = sql_statement.columns(opt_id=Integer, *columns)
        query_result = self.execute_query(sql_statement, optimization_ids=list(optimization_ids))

        ret = {}
        for rec in query_result:
            self._remove_excluded_keys(rec)
            key = rec.pop('opt_id')
            ret[key] = Molecule(**rec)

        return ret
