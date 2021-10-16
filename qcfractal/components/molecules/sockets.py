from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcelemental.molutil import order_molecular_formula
from sqlalchemy.orm import load_only
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql import select, and_, or_

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.db_socket.helpers import (
    get_count_2,
    get_query_proj_columns,
    insert_general,
    delete_general,
    insert_mixed_general,
    get_general,
    calculate_limit,
)
from qcfractal.exceptions import LimitExceededError, MissingDataError
from qcfractal.portal.components.molecules import Molecule, MoleculeIdentifiers
from qcfractal.portal.metadata_models import (
    InsertMetadata,
    DeleteMetadata,
    QueryMetadata,
    UpdateMetadata,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Union, Tuple, Optional, Sequence, Iterable, Dict, Any

    MoleculeDict = Dict[str, Any]


class MoleculeSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.molecule

    @staticmethod
    def molecule_to_orm(molecule: Molecule) -> MoleculeORM:

        # Validate the molecule if it hasn't been validated already
        if molecule.validated is False:
            molecule = Molecule(**molecule.dict(), validate=True)

        mol_dict = molecule.dict(exclude={"id", "validated", "fix_com", "fix_orientation"})

        # Build these quantities fresh from what is actually stored
        mol_dict["molecule_hash"] = molecule.get_hash()

        mol_dict.setdefault("identifiers", dict())
        mol_dict["identifiers"]["molecule_hash"] = mol_dict["molecule_hash"]
        mol_dict["identifiers"]["molecular_formula"] = molecule.get_molecular_formula()

        mol_dict["fix_com"] = True
        mol_dict["fix_orientation"] = True

        return MoleculeORM(**mol_dict)  # type: ignore

    def add(
        self, molecules: Sequence[Molecule], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Add molecules to the database

        This checks if the molecule already exists in the database via its hash. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        molecules
            Molecule data to add to the session
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

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

        with self.root_socket.optional_session(session) as session:
            meta, added_ids = insert_general(session, molecule_orm, (MoleculeORM.molecule_hash,), (MoleculeORM.id,))

        # added_ids is a list of tuple, with each tuple only having one value. Flatten that out
        return meta, [x[0] for x in added_ids]

    def get(
        self,
        id: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
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
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Molecule information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        if len(id) > self._limit:
            raise LimitExceededError(f"Request for {len(id)} molecules is over the limit of {self._limit}")

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, MoleculeORM, MoleculeORM.id, id, include, exclude, None, missing_ok)

    def add_mixed(
        self, molecule_data: Sequence[Union[int, Molecule]], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Add a mixed format molecule specification to the database.

        This function can take both Molecule objects and molecule ids. If a molecule id is given
        in the list, then it is checked to make sure it exists. If it does not exist, then it will be
        marked as an error in the returned metadata and the corresponding entry in the returned
        list of IDs will be None.

        If a Molecule object is given, it will be added to the database if it does not already exist
        in the database (based on the hash) and the existing ID will be returned. Otherwise, the new
        ID will be returned.

        Parameters
        ----------
        molecule_data
            Molecule data to add. Can be a mix of IDs and Molecule objects
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        molecule_orm: List[Union[int, MoleculeORM]] = [
            x if isinstance(x, int) else self.molecule_to_orm(x) for x in molecule_data
        ]

        with self.root_socket.optional_session(session) as session:
            meta, all_ids = insert_mixed_general(
                session, MoleculeORM, molecule_orm, MoleculeORM.id, (MoleculeORM.molecule_hash,), (MoleculeORM.id,)
            )

        # added_ids is a list of tuple, with each tuple only having one value. Flatten that out
        return meta, [x[0] if x is not None else None for x in all_ids]

    def delete(self, id: Sequence[int], *, session: Optional[Session] = None) -> DeleteMetadata:
        """
        Removes molecules from the database based on id

        Parameters
        ----------
        id
            IDs of the molecules to remove
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Information about what was deleted and any errors that occurred
        """

        id_lst = [(x,) for x in id]

        with self.root_socket.optional_session(session) as session:
            return delete_general(session, MoleculeORM, (MoleculeORM.id,), id_lst)

    def query(
        self,
        id: Optional[Iterable[int]] = None,
        molecule_hash: Optional[Iterable[str]] = None,
        molecular_formula: Optional[Iterable[str]] = None,
        identifiers: Optional[Dict[str, Iterable[Any]]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
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
        identifiers
            Query based on identifiers. Dictionary is identifier name -> value
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
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        if molecular_formula is not None:
            try:
                # Make sure the molecular formaulae are in the proper element order
                molecular_formula = [order_molecular_formula(form) for form in molecular_formula]
            except ValueError:
                # Probably, the user provided an invalid chemical formula
                pass

        limit = calculate_limit(self._limit, limit)

        load_cols, _ = get_query_proj_columns(MoleculeORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(MoleculeORM.id.in_(id))
        if molecule_hash is not None:
            and_query.append(MoleculeORM.molecule_hash.in_(molecule_hash))
        if molecular_formula is not None:
            # Add it to the identifiers query
            if identifiers is None:
                identifiers = {"molecular_formula": molecular_formula}
            else:
                identifiers["molecular_formula"] = molecular_formula
        if identifiers is not None:
            for i_name, i_values in identifiers.items():
                or_query = []
                for v in i_values:
                    or_query.append(MoleculeORM.identifiers.contains({i_name: v}))
                and_query.append(or_(*or_query))

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(MoleculeORM).where(and_(*and_query))
            stmt = stmt.options(load_only(*load_cols))
            n_found = get_count_2(session, stmt)
            stmt = stmt.limit(limit).offset(skip)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def modify(
        self,
        id: int,
        name: Optional[str] = None,
        comment: Optional[str] = None,
        identifiers: Optional[MoleculeIdentifiers] = None,
        overwrite_identifiers: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Modify molecule information in the database

        This is only capable of updating the name, comment, and identifiers fields (except molecule_hash
        and molecular formula).

        If a molecule with that id does not exist, an exception is raised

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        id
            Molecule ID of the molecule to modify
        name
            New name for the molecule. If None, name is not changed.
        comment
            New comment for the molecule. If None, comment is not changed
        identifiers
            A new set of identifiers for the molecule
        overwrite_identifiers
            If True, the identifiers of the molecule are set to be those given exactly (ie, identifiers
            that exist in the DB but not in the new set will be removed). Otherwise, the new set of
            identifiers is merged into the existing ones. Note that molecule_hash and molecular_formula
            are never removed.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        with self.root_socket.optional_session(session) as session:
            stmt = select(MoleculeORM).where(or_(MoleculeORM.id == id)).with_for_update()
            stmt = stmt.options(
                load_only(MoleculeORM.id, MoleculeORM.name, MoleculeORM.comment, MoleculeORM.identifiers)
            )
            mol = session.execute(stmt).scalar_one_or_none()

            if mol is None:
                raise MissingDataError(f"Molecule with id {id} not found in the database")

            if name is not None:
                mol.name = name
            if comment is not None:
                mol.comment = comment
            if identifiers is not None:
                id_dict = identifiers.dict()
                id_dict["molecule_hash"] = mol.identifiers["molecule_hash"]
                id_dict["molecular_formula"] = mol.identifiers["molecular_formula"]

                # Changing identifiers is a bit sensitive, so validate again
                identifiers = MoleculeIdentifiers(**id_dict)

                if overwrite_identifiers:
                    # Always keep hash & formula
                    mol.identifiers = identifiers.dict()
                else:
                    mol.identifiers.update(identifiers.dict())

                    # sqlalchemy cannot track changes in json
                    flag_modified(mol, "identifiers")

        return UpdateMetadata(updated_idx=[0])  # type: ignore
