"""
SQLAlchemy Database class to handle access to Pstgres through ORM
"""


try:
    import sqlalchemy
except ImportError:
    raise ImportError(
        "SQLAlchemy_socket requires sqlalchemy, please install this python module or try a different db_socket.")



from sqlalchemy import create_engine
from .sql_models import Base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager



import logging
import secrets
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Union
from sqlalchemy.sql import select, column

from .sql_models import (CollectionORM, KeywordsORM, LogsORM, MoleculeORM,
                         OptimizationProcedureORM, QueueManagerORM, ResultORM,
                         ServiceQueueORM, TaskQueueORM, UserORM)
from .storage_utils import add_metadata_template, get_metadata_template
from ..interface.models import KeywordSet, Molecule, ResultRecord, prepare_basis


_null_keys = {"basis", "keywords"}
_id_keys = {"id", "molecule", "keywords", "procedure_id"}
_lower_func = lambda x: x.lower()
_prepare_keys = {"program": _lower_func, "basis": prepare_basis, "method": _lower_func, "procedure": _lower_func}


def dict_from_tuple(keys, values):
    return [dict(zip(keys, row)) for row in values]

def format_query(ORMClass, **query: Dict[str, Union[str, List[str]]]) -> Dict[str, Union[str, List[str]]]:
    """
    Formats a query into a SQLAlchemy format.
    """

    ret = []
    for k, v in query.items():
        if v is None:
            continue

        # Handle None keys
        k = k.lower()
        if (k in _null_keys) and (v == 'null'):
            v = None


        if k in _prepare_keys:
            f = _prepare_keys[k]
            if isinstance(v, (list, tuple)):
                v = [f(x) for x in v]
            else:
                v = f(v)

        if isinstance(v, (list, tuple)):
            col = getattr(ORMClass, k)
            ret.append(getattr(col, "in_")(v))
        else:
            ret.append(getattr(ORMClass, k) == v)

    return ret


class SQLAlchemySocket:
    """
        SQLAlcehmy QCDB wrapper class.
    """

    def __init__(self,
                 uri: str,
                 project: str="molssidb",
                 bypass_security: bool=False,
                 allow_read: bool=True,
                 logger: 'Logger'=None,
                 sql_echo: bool= False,
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
        self.engine = create_engine(uri,
                                    echo=sql_echo,  # echo for logging into python logging
                                    pool_size=5  # 5 is the default, 0 means unlimited
                                    )
        self.logger.info('Connected SQLAlchemy to DB dialect {} with driver {}'.format(
            self.engine.dialect.name, self.engine.driver))

        self.Session = sessionmaker(bind=self.engine)

        # actually create the tables
        Base.metadata.create_all(self.engine)

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

        # drop all tables that it knows about
        Base.metadata.drop_all(self.engine)

        # create the tables again
        Base.metadata.create_all(self.engine)

        # self.client.drop_database(db_name)

    def get_project_name(self) -> str:
        return self._project_name

    def get_limit(self, limit: Optional[int]) -> int:
        """Get the allowed limit on results to return in queries based on the
         given `limit`. If this number is greater than the
         mongoengine_soket.max_limit then the max_limit will be returned instead.
        """

        return limit if limit and limit < self._max_limit else self._max_limit

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Logs (KV store) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_logs(self, blobs_list: List[Any]):
        """
        Adds to the key/value store table.

        Parameters
        ----------
        blobs_list : List[Any]
            A list of data blobs to add.

        Returns
        -------
        TYPE

            Description
        """

        meta = add_metadata_template()
        blob_ids = []
        for blob in blobs_list:
            if blob is None:
                blob_ids.append(None)
                continue

            doc = LogsORM(value=blob)
            doc.save()
            blob_ids.append(str(doc.id))
            meta['n_inserted'] += 1

        meta["success"] = True

        return {"data": blob_ids, "meta": meta}

    def get_logs(self, id: List[str]):
        """
        Pulls from the key/value store table.

        Parameters
        ----------
        id : List[str]
            A list of ids to query

        Returns
        -------
        TYPE
            Description
        """

        meta = get_metadata_template()

        query, errors = format_query(id=id)

        data = LogsORM.objects(**query)

        meta["success"] = True
        meta["n_found"] = data.count()  # all data count, can be > len(data)
        meta["errors"].extend(errors)

        data = [d.to_json_obj() for d in data]
        data = {d["id"]: d["value"] for d in data}
        return {"data": data, "meta": meta}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Molecule ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_add_molecules_mixed(self, data: List[Union[str, Molecule]]) -> List[Molecule]:
        """
        Get or add the given molecules (if they don't exit).
        MoleculeORMs are given in a mixed format, either as a dict of mol data
        or as existing mol id

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ordered_mol_dict = {indx: mol for indx, mol in enumerate(data)}
        new_molecules = {}
        id_mols = {}
        for idx, mol in ordered_mol_dict.items():
            if isinstance(mol, str):
                id_mols[idx] = mol
            elif isinstance(mol, Molecule):
                new_molecules[idx] = mol
            else:
                meta["errors"].append((idx, "Data type not understood"))

        ret_mols = {}

        # Add all new molecules
        flat_mols = []
        flat_mol_keys = []
        for k, v in new_molecules.items():
            flat_mol_keys.append(k)
            flat_mols.append(v)
        flat_mols = self.add_molecules(flat_mols)["data"]
        id_mols.update({k: v for k, v in zip(flat_mol_keys, flat_mols)})

        # Get molecules by index and translate back to dict
        tmp = self.get_molecules(list(id_mols.values()))
        id_mols_list = tmp["data"]
        meta["errors"].extend(tmp["meta"]["errors"])

        inv_id_mols = {v: k for k, v in id_mols.items()}

        for mol in id_mols_list:
            ret_mols[inv_id_mols[mol.id]] = mol

        meta["success"] = True
        meta["n_found"] = len(ret_mols)
        meta["missing"] = list(ordered_mol_dict.keys() - ret_mols.keys())

        # Rewind to flat last
        ret = []
        for ind in range(len(ordered_mol_dict)):
            if ind in ret_mols:
                ret.append(ret_mols[ind])
            else:
                ret.append(None)

        return {"meta": meta, "data": ret}


    def add_molecules(self, molecules: List[Molecule]):
        """
        Adds molecules to the database.

        Parameters
        ----------
        data : dict of molecule-like JSON objects
            A {key: molecule} dictionary of molecules to input.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        meta = add_metadata_template()

        results = []
        with self.session_scope() as session:
            for dmol in molecules:

                mol_dict = dmol.json_dict(exclude={"id"})

                # TODO: can set them as defaults in the sql_models, not here
                mol_dict["fix_com"] = True
                mol_dict["fix_orientation"] = True

                # Build fresh indices
                mol_dict["molecule_hash"] = dmol.get_hash()
                mol_dict["molecular_formula"] = dmol.get_molecular_formula()

                mol_dict["identifiers"] = {}
                mol_dict["identifiers"]["molecule_hash"] = mol_dict["molecule_hash"]
                mol_dict["identifiers"]["molecular_formula"] = mol_dict["molecular_formula"]

                # search by index keywords not by all keys, much faster

                doc = session.query(MoleculeORM).filter_by(molecule_hash=mol_dict['molecule_hash'])

                if doc.count() == 0:
                    doc = MoleculeORM(**mol_dict)
                    session.add(doc)
                    # Todo: commit at the end, but list itself might have duplicates
                    session.commit()
                    results.append(str(doc.id))
                    meta['n_inserted'] += 1
                else:

                    id = str(doc.first().id)
                    meta['duplicates'].append(id)  # TODO
                    # If new or duplicate, add the id to the return list
                    results.append(id)

                    # We should make sure there was not a hash collision?
                    # new_mol.compare(old_mol)
                    # raise KeyError("!!! WARNING !!!: Hash collision detected")
        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def get_molecules(self, id=None, molecule_hash=None, molecular_formula=None, limit: int=None, skip: int=0):

        ret = {"meta": get_metadata_template(), "data": []}

        query = format_query(MoleculeORM, id=id, molecule_hash=molecule_hash, molecular_formula=molecular_formula)
        # query = [getattr(MoleculeORM, 'id') == id,
        #          MoleculeORM.molecule_hash == molecule_hash,
        #          MoleculeORM.molecular_formula == molecular_formula
        # ]

        # Make the query
        with self.session_scope() as session:
            data = session.query(MoleculeORM).filter(*query)\
                                        .limit(self.get_limit(limit))\
                                        .offset(skip)

            ret["meta"]["success"] = True
            ret["meta"]["n_found"] = data.count()  # TODO: should return count(*)
            # ret["meta"]["errors"].extend(errors)
            data = data.all()

            # Don't include the hash or the molecular_formula in the returned result
            # Todo: tobe removed after bug is fixed in elemental
            for d in data:
                if d.connectivity is None:
                    d.connectivity = []
            data = [Molecule(**d.to_dict(exclude=['molecule_hash', 'molecular_formula']), validate=False)
                    for d in data]
            ret["data"] = data

        return ret

    def del_molecules(self, id: List[str]=None, molecule_hash: List[str]=None):
        """
        Removes a molecule from the database from its hash.

        Parameters
        ----------
        values : str or list of strs
            The hash of a molecule.

        Returns
        -------
        bool
            Number of deleted molecules.
        """

        query = format_query(MoleculeORM, id=id, molecule_hash=molecule_hash)

        with self.session_scope() as session:
            ret = session.query(MoleculeORM).filter(*query)\
                .delete(synchronize_session=False)

        return ret


# ~~~~~~~~~~~~~~~~~~~~~~~ Keywords ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_keywords(self, keyword_sets: List[KeywordSet]):
        """Add one KeywordSet uniquly identified by 'program' and the 'name'.

        Parameters
        ----------
         data
            A list of KeywordSets to be inserted.

        Returns
        -------
            A dict with keys: 'data' and 'meta'
            (see add_metadata_template())
            The 'data' part is a list of ids of the inserted options
            data['duplicates'] has the duplicate entries

        Notes
        ------
            Duplicates are not considered errors.

        """

        meta = add_metadata_template()

        keywords = []
        with self.session_scope() as session:
            for kw in keyword_sets:

                kw_dict = kw.json_dict(exclude={"id"})

                # search by index keywords not by all keys, much faster
                found = session.query(KeywordsORM).filter_by(hash_index=kw_dict['hash_index']).first()
                if not found:
                    doc = KeywordsORM(**kw_dict)
                    session.add(doc)
                    session.commit()
                    keywords.append(str(doc.id))
                    meta['n_inserted'] += 1
                else:
                    meta['duplicates'].append(str(found.id))  # TODO
                    keywords.append(str(found.id))
                meta["success"] = True

        ret = {"data": keywords, "meta": meta}

        return ret

    def get_keywords(self,
                     id: Union[str, list]=None,
                     hash_index: Union[str, list]=None,
                     limit: int=None,
                     skip: int=0,
                     return_json: bool=True,
                     with_ids: bool=True) -> List[KeywordSet]:
        """Search for one (unique) option based on the 'program'
        and the 'name'. No overwrite allowed.

        Parameters
        ----------
        id : list or str
            Ids of the keywords
        hash_index : list or str
            hash index of keywords
        limit : int, optional
            Maximum number of results to return.
            If this number is greater than the mongoengine_soket.max_limit then
            the max_limit will be returned instead.
            Default is to return the socket's max_limit (when limit=None or 0)
        skip : int, optional
        return_json : bool, optional
            Return the results as a json object
            Default is True
        with_ids : bool, optional
            Include the DB ids in the returned object (names 'id')
            Default is True


        Returns
        -------
            A dict with keys: 'data' and 'meta'
            (see get_metadata_template())
            The 'data' part is an object of the result or None if not found
        """

        meta = get_metadata_template()
        query = format_query(KeywordsORM, id=id, hash_index=hash_index)

        with self.session_scope() as session:
            data = session.query(KeywordsORM).filter(*query)\
                                             .limit(self.get_limit(limit))\
                                             .offset(skip)

            meta["n_found"] = data.count()
            meta["success"] = True

            # meta['error_description'] = str(err)
            data = data.all()
            if return_json:
                rdata = [KeywordSet(**d.to_json_obj(with_ids)) for d in data]
            else:
                rdata = data

        return {"data": rdata, "meta": meta}

    def get_add_keywords_mixed(self, data):
        """
        Get or add the given options (if they don't exit).
        KeywordsORM are given in a mixed format, either as a dict of mol data
        or as existing mol id

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ids = []
        for idx, kw in enumerate(data):
            if isinstance(kw, str):
                ids.append(kw)

            elif isinstance(kw, KeywordSet):
                new_id = self.add_keywords([kw])["data"][0]
                ids.append(new_id)
            else:
                meta["errors"].append((idx, "Data type not understood"))
                ids.append(None)

        missing = []
        ret = []
        for idx, id in enumerate(ids):
            if id is None:
                ret.append(None)
                missing.append(idx)
                continue

            tmp = self.get_keywords(id=id)["data"]
            if tmp:
                ret.append(tmp[0])
            else:
                ret.append(None)

        meta["success"] = True
        meta["n_found"] = len(ret) - len(missing)
        meta["missing"] = missing

        return {"meta": meta, "data": ret}

    def del_keywords(self, id: str) -> int:
        """
        Removes a option set from the database based on its id.

        Parameters
        ----------
        id : str
            id of the keyword

        Returns
        -------
        int
           number of deleted documents
        """

        count = 0
        with self.session_scope() as session:
            count = session.query(KeywordsORM).filter_by(id=id).delete(synchronize_session=False)

        return count
