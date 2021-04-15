from __future__ import annotations

import logging
import qcelemental

from sqlalchemy import and_
from qcfractal.storage_sockets.models import MoleculeORM
from qcfractal.interface.models import Molecule, ObjectId, InsertMetadata, DeleteMetadata, QueryMetadata
from qcfractal.storage_sockets.sqlalchemy_socket import calculate_limit
from qcfractal.storage_sockets.sqlalchemy_common import (
    get_count,
    get_query_proj_columns,
    insert_general,
    delete_general,
    insert_mixed_general,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Union, Tuple, Optional, Sequence, Iterable, Dict, Any

    MoleculeDict = Dict[str, Any]


class MoleculeSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.molecule

    @staticmethod
    def molecule_to_orm(molecule: Molecule) -> MoleculeORM:

        # Validate the molecule if it hasn't been validated already
        if molecule.validated is False:
            molecule = Molecule(**molecule.dict(), validate=True)

        mol_dict = molecule.dict(exclude={"id", "validated"})

        # TODO: can set them as defaults in the sql_models, not here
        mol_dict["fix_com"] = True
        mol_dict["fix_orientation"] = True

        # Build these quantities fresh from what is actually stored
        mol_dict["molecule_hash"] = molecule.get_hash()
        mol_dict["molecular_formula"] = molecule.get_molecular_formula()

        mol_dict["identifiers"] = {
            "molecule_hash": mol_dict["molecule_hash"],
            "molecular_formula": mol_dict["molecular_formula"],
        }

        return MoleculeORM(**mol_dict)  # type: ignore

    @staticmethod
    def cleanup_molecule_dict(mol_dict: MoleculeDict) -> MoleculeDict:
        """
        Removes any fields of a molecule dictionary that are None
        """

        # TODO - int id
        if "id" in mol_dict:
            mol_dict["id"] = ObjectId(mol_dict["id"])

        # Also, remove any values that are None/Null in the db
        # The molecule pydantic model cannot always handle these, so let the model handle the defaults
        return {k: v for k, v in mol_dict.items() if v is not None}

    def add_internal(self, session: Session, molecules: Sequence[Molecule]) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Add molecules to a session

        This checks if the molecule already exists in the database via its hash. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Changes are not committed to to the database, but they are flushed.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to add the data to
        molecules
            Molecule data to add to the session

        Returns
        -------
        :
            Metadata about the insertion, and a list of Molecule ids. The ids will be in the
            order of the input molecules.
        """

        ###############################################################################
        # Exceptions in this function would usually be a programmer error, as any
        # valid Molecule object should be insertable into the database
        ###############################################################################

        molecule_orm = [self.molecule_to_orm(x) for x in molecules]

        meta, added_ids = insert_general(session, molecule_orm, (MoleculeORM.molecule_hash,), (MoleculeORM.id,))

        # insert_general should always succeed or raise exception
        assert meta.success

        # Added ids are a list of tuple, with each tuple only having one value
        return meta, [ObjectId(x[0]) for x in added_ids]

    def get(
        self,
        id: Sequence[ObjectId],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
    ) -> List[Optional[MoleculeDict]]:
        """
        Obtain molecules from with specified IDs

        The returned molecule information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of Molecules will be None.

        Parameters
        ----------
        id
            A list or other sequence of molecule IDs
        include
            Which fields of the molecule to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, then missing molecules will be tolerated, and the returned list of
           Molecules will contain None for the corresponding IDs that were not found.

        Returns
        -------
        :
            Molecule information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} molecules is over the limit of {self._limit}")

        # TODO - int id
        id = [int(x) for x in id]

        unique_ids = list(set(id))

        # TODO - remove this eventually
        # right now, a lot of code depends on these fields not being here
        # but that is not right. After changing the client code to be more dict-oriented,
        # then we can add these back
        if exclude is not None:
            exclude = list(exclude) + ["molecule_hash", "molecular_formula"]
        else:
            exclude = ["molecule_hash", "molecular_formula"]

        query_cols_names, query_cols = get_query_proj_columns(MoleculeORM, include, exclude)

        with self._core_socket.session_scope(True) as session:
            results = session.query(MoleculeORM.id, *query_cols).filter(MoleculeORM.id.in_(unique_ids)).all()

            # x[0] is the id, the rest are the columns we want to return
            # we zip it with the column names to form our dictionary
            result_map = {x[0]: dict(zip(query_cols_names, x[1:])) for x in results}

        result_map = {k: self.cleanup_molecule_dict(v) for k, v in result_map.items()}

        # Put into the original order
        ret = [result_map.get(x, None) for x in id]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested molecule records")

        return ret

    def add(self, molecules: Sequence[Molecule]) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds molecules to the database.

        This function returns metadata about the insertion, and a list of IDs of the molecules in the database.
        This list will be in the same order as the `molecules` parameter.

        For each molecule given, the database will be checked to see if it already exists in the database.
        If it does, that ID will be returned. Otherwise, the molecule will be added to the database and the
        new ID returned.

        On any error, this function will raise an exception.

        Parameters
        ----------
        molecules
            A List of Molecule objects to add.

        Returns
        -------
        :
            Metadata about the insertion, and a list of Molecule ids. The ids will be in the
            order of the input molecules.
        """

        with self._core_socket.session_scope() as session:
            return self.add_internal(session, molecules)

    def add_mixed(
        self, molecule_data: Sequence[Union[ObjectId, Molecule]]
    ) -> Tuple[InsertMetadata, List[Optional[ObjectId]]]:
        """
        Add a mixed format molecule specification to the database.

        This function can take both Molecule objects and molecule ids. If a molecule id is given
        in the list, then it is checked to make sure it exists. If it does not exist, then it will be
        marked as an error in the returned metadata and the corresponding entry in the returned
        list of IDs will be None.

        If a Molecule object is given, it will be added to the database if it does not already exist
        in the database (based on the hash) and the existing ID will be returned. Otherwise, the new
        ID will be returned.
        """

        # TODO - INT ID
        molecule_data_2 = [int(x) if isinstance(x, (int, str, ObjectId)) else x for x in molecule_data]

        molecule_orm: List[Union[int, MoleculeORM]] = [
            x if isinstance(x, int) else self.molecule_to_orm(x) for x in molecule_data_2
        ]

        with self._core_socket.session_scope() as session:
            meta, all_ids = insert_mixed_general(
                session, MoleculeORM, molecule_orm, MoleculeORM.id, (MoleculeORM.molecule_hash,), (MoleculeORM.id,)
            )

        # all_ids is a list of Tuples
        # TODO - INT ID
        return meta, [ObjectId(x[0]) if x is not None else None for x in all_ids]

    def delete(self, id: List[ObjectId]) -> DeleteMetadata:
        """
        Removes molecules from the database based on id

        Parameters
        ----------
        id
            IDs of the molecules to remove

        Returns
        -------
        :
            Information about what was deleted and any errors that occurred
        """

        # TODO - INT ID
        id_lst = [(int(x),) for x in id]

        with self._core_socket.session_scope() as session:
            # session will commit on exiting from the context
            return delete_general(session, MoleculeORM, (MoleculeORM.id,), id_lst)

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        molecule_hash: Optional[Iterable[str]] = None,
        molecular_formula: Optional[Iterable[str]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, List[MoleculeDict]]:
        """
        General query of molecules in the database

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        id
            Query for molecules based on its ID
        molecule_hash
            Query for molecules based on its hash
        molecular_formula
            Query for molecules based on molecular formula
        include
            Which fields of the molecule to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        limit
            Limit the number of results. If None, the server limit will be used.
            This limit will not be respected if greater than the configured limit of the server.
        skip
            Skip this many results from the total list of matches. The limit will apply after skipping,
            allowing for pagination.

        Returns
        -------
        :
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        if molecular_formula is not None:
            try:
                # Make sure the molecular formaulae are in the proper element order
                molecular_formula = [qcelemental.molutil.order_molecular_formula(form) for form in molecular_formula]
            except ValueError:
                # Probably, the user provided an invalid chemical formula
                pass

        limit = calculate_limit(self._limit, limit)

        # TODO - remove this eventually
        # right now, a lot of code depends on these fields not being here
        # but that is not right. After changing the client code to be more dict-oriented,
        # then we can add these back
        if exclude is not None:
            exclude = list(exclude) + ["molecule_hash", "molecular_formula"]
        else:
            exclude = ["molecule_hash", "molecular_formula"]

        query_cols_names, query_cols = get_query_proj_columns(MoleculeORM, include, exclude)

        and_query = []
        if molecular_formula is not None:
            and_query.append(MoleculeORM.molecular_formula.in_(molecular_formula))
        if molecule_hash is not None:
            and_query.append(MoleculeORM.molecule_hash.in_(molecule_hash))
        if id is not None:
            and_query.append(MoleculeORM.id.in_(id))

        with self._core_socket.session_scope(True) as session:
            query = session.query(*query_cols).filter(and_(*and_query))
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).all()
            result_dicts = [dict(zip(query_cols_names, x)) for x in results]

        # Remove any values that are None/Null in the db
        # The molecule pydantic model cannot always handle these, so let the model
        # handle the defaults
        result_dicts = list(map(self.cleanup_molecule_dict, result_dicts))

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts
