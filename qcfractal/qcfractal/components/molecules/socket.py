from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcelemental.molutil import order_molecular_formula
from sqlalchemy.orm import load_only
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql import select, and_, or_

from qcfractal.db_socket.helpers import (
    insert_general,
    delete_general,
    insert_mixed_general,
    get_general,
)
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import (
    InsertMetadata,
    DeleteMetadata,
    UpdateMetadata,
)
from qcportal.molecules import Molecule, MoleculeIdentifiers, MoleculeQueryFilters
from .db_models import MoleculeORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Union, Tuple, Optional, Sequence, Dict, Any


# Basically random, but unique to molecules
molecule_insert_lock_id = 24773


class MoleculeSocket:
    """
    Socket for managing/querying molecules
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def molecule_to_orm(molecule: Molecule) -> MoleculeORM:
        """
        Convert a pydantic (QCElemental) Molecule to an ORM
        """

        # Validate the molecule if it hasn't been validated already
        if molecule.validated is False:
            molecule = Molecule(**molecule.dict(), validate=True)

        mol_dict = molecule.dict(exclude={"id", "validated", "fix_com", "fix_orientation"})

        # Build these quantities fresh from what is actually stored
        mol_dict["molecule_hash"] = molecule.get_hash()

        mol_dict.setdefault("identifiers", dict())
        mol_dict["identifiers"]["molecule_hash"] = mol_dict["molecule_hash"]
        mol_dict["identifiers"]["molecular_formula"] = molecule.get_molecular_formula()

        return MoleculeORM(**mol_dict)

    def add(
        self, molecules: Sequence[Molecule], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Add molecules to the database

        This checks if the molecule already exists in the database via its hash. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        molecules
            Molecule data to add to the session
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of Molecule ids.
            The ids will be in the order of the input molecules.
        """

        ###############################################################################
        # Exceptions in this function would usually be a programmer error, as any
        # valid Molecule object should be insertable into the database
        ###############################################################################

        molecule_orm = [self.molecule_to_orm(x) for x in molecules]

        with self.root_socket.optional_session(session) as session:
            # lock id is basically random, but unique to molecules
            meta, added_ids = insert_general(
                session, molecule_orm, (MoleculeORM.molecule_hash,), (MoleculeORM.id,), lock_id=molecule_insert_lock_id
            )

        # added_ids is a list of tuple, with each tuple only having one value. Flatten that out
        return meta, [x[0] for x in added_ids]

    def get(
        self,
        molecule_id: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Obtain molecules with specified IDs from the database

        Parameters
        ----------
        molecule_id
            A list or other sequence of molecule IDs
        include
            Which fields of the molecule to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
            If set to True, then missing molecules will be tolerated, and the returned list of
            Molecules will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            List of molecule data (as dictionaries) in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, MoleculeORM, MoleculeORM.id, molecule_id, include, exclude, missing_ok)

    def add_mixed(
        self, molecule_data: Sequence[Union[int, Molecule]], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Add mixed molecules and ids to the database.

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
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of Molecule ids.
            The ids will be in the order of the input molecules.
        """

        molecule_orm: List[Union[int, MoleculeORM]] = [
            x if isinstance(x, int) else self.molecule_to_orm(x) for x in molecule_data
        ]

        with self.root_socket.optional_session(session) as session:
            meta, all_ids = insert_mixed_general(
                session,
                MoleculeORM,
                molecule_orm,
                MoleculeORM.id,
                (MoleculeORM.molecule_hash,),
                (MoleculeORM.id,),
                lock_id=molecule_insert_lock_id,
            )

        # added_ids is a list of tuple, with each tuple only having one value. Flatten that out
        return meta, [x[0] if x is not None else None for x in all_ids]

    def delete(self, molecule_id: Sequence[int], *, session: Optional[Session] = None) -> DeleteMetadata:
        """
        Removes molecules with the given ids from the database

        Parameters
        ----------
        molecule_id
            IDs of the molecules to remove
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about what was deleted and any errors that occurred
        """

        id_lst = [(x,) for x in molecule_id]

        with self.root_socket.optional_session(session) as session:
            return delete_general(session, MoleculeORM, MoleculeORM.id, id_lst)

    def query(
        self,
        query_data: MoleculeQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        General query of molecules in the database

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        query_data
            Fields/filters to query for
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of molecule ids that were found in the database.
        """

        molecular_formula = query_data.molecular_formula
        identifiers = query_data.identifiers

        if molecular_formula is not None:
            try:
                # Make sure the molecular formulae are in the proper element order
                molecular_formula = [order_molecular_formula(form) for form in query_data.molecular_formula]
            except ValueError:
                # Probably, the user provided an invalid chemical formula
                pass

        and_query = []
        if query_data.molecule_id is not None:
            and_query.append(MoleculeORM.id.in_(query_data.molecule_id))
        if query_data.molecule_hash is not None:
            and_query.append(MoleculeORM.molecule_hash.in_(query_data.molecule_hash))
        if molecular_formula is not None:
            # Add it to the identifiers query
            if identifiers is None:
                identifiers = {"molecular_formula": list(molecular_formula)}
            else:
                identifiers["molecular_formula"] = list(molecular_formula)
        if identifiers is not None:
            for i_name, i_values in identifiers.items():
                or_query = []
                for v in i_values:
                    or_query.append(MoleculeORM.identifiers.contains({i_name: v}))
                and_query.append(or_(False, *or_query))

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(MoleculeORM.id).where(and_(True, *and_query))

            if query_data.cursor is not None:
                stmt = stmt.where(MoleculeORM.id < query_data.cursor)

            stmt = stmt.order_by(MoleculeORM.id.desc())
            stmt = stmt.limit(query_data.limit)
            stmt = stmt.distinct(MoleculeORM.id)
            molecule_ids = session.execute(stmt).scalars().all()

        return molecule_ids

    def modify(
        self,
        molecule_id: int,
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

        Parameters
        ----------
        molecule_id
            Molecule ID of the molecule to modify
        name
            New name for the molecule. If None, name is not changed.
        comment
            New comment for the molecule. If None, comment is not changed
        identifiers
            New identifiers for the molecule
        overwrite_identifiers
            If True, the identifiers of the molecule are set to be those given exactly (ie, identifiers
            that exist in the DB but not in the new set will be removed). Otherwise, the new set of
            identifiers is merged into the existing ones. Note that molecule_hash and molecular_formula
            are never removed.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        with self.root_socket.optional_session(session) as session:
            stmt = select(MoleculeORM).where(or_(MoleculeORM.id == molecule_id)).with_for_update()
            stmt = stmt.options(
                load_only(MoleculeORM.id, MoleculeORM.name, MoleculeORM.comment, MoleculeORM.identifiers)
            )
            mol = session.execute(stmt).scalar_one_or_none()

            if mol is None:
                raise MissingDataError(f"Molecule with id {molecule_id} not found in the database")

            if name is not None:
                mol.name = name
            if comment is not None:
                mol.comment = comment
            if identifiers is not None:
                update_dict = {
                    "molecule_hash": mol.identifiers["molecule_hash"],
                    "molecular_formula": mol.identifiers["molecular_formula"],
                }

                # Changing identifiers is a bit sensitive, so validate again
                identifiers = identifiers.copy(update=update_dict)

                if overwrite_identifiers:
                    # Always keep hash & formula
                    mol.identifiers = identifiers.dict(exclude_unset=True, exclude_defaults=True, exclude_none=True)
                else:
                    id_dict = identifiers.dict(exclude_unset=True, exclude_defaults=True)
                    mol.identifiers.update(id_dict)

                    # sqlalchemy cannot track changes in json
                    flag_modified(mol, "identifiers")

        return UpdateMetadata(updated_idx=[0])
