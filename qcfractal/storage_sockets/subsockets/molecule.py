from __future__ import annotations

import qcelemental

from qcfractal.storage_sockets.models import MoleculeORM
from qcfractal.interface.models import Molecule
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.interface.models import ObjectId
    from typing import List, Union


class MoleculeSocket:
    def __init__(self, core_socket):
        self._core_socket = core_socket

    def get_add_mixed(self, data: List[Union[ObjectId, Molecule]]) -> List[Molecule]:
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
            if isinstance(mol, (int, str)):
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
        flat_mols = self.add(flat_mols)["data"]

        id_mols.update({k: v for k, v in zip(flat_mol_keys, flat_mols)})

        # Get molecules by index and translate back to dict
        tmp = self.get(list(id_mols.values()))
        id_mols_list = tmp["data"]
        meta["errors"].extend(tmp["meta"]["errors"])

        # TODO - duplicate ids get removed on the line below. Some
        # code may depend on this behavior, so careful changing it
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

    def add(self, molecules: List[Molecule]):
        """
        Adds molecules to the database.

        Parameters
        ----------
        molecules : List[Molecule]
            A List of molecule objects to add.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        meta = add_metadata_template()

        with self._core_socket.session_scope() as session:

            # Build out the ORMs
            orm_molecules = []
            for dmol in molecules:

                if dmol.validated is False:
                    dmol = Molecule(**dmol.dict(), validate=True)

                mol_dict = dmol.dict(exclude={"id", "validated"})

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
                orm_molecules.append(MoleculeORM(**mol_dict))

            # Check if we have duplicates
            hash_list = [x.molecule_hash for x in orm_molecules]
            query = format_query(MoleculeORM, molecule_hash=hash_list)
            indices = session.query(MoleculeORM.molecule_hash, MoleculeORM.id).filter(*query)
            previous_id_map = {k: v for k, v in indices}

            # For a bulk add there must be no pre-existing and there must be no duplicates in the add list
            bulk_ok = len(hash_list) == len(set(hash_list))
            bulk_ok &= len(previous_id_map) == 0
            # bulk_ok = False

            if bulk_ok:
                # Bulk save, doesn't update fields for speed
                session.bulk_save_objects(orm_molecules)
                session.commit()

                # Query ID's and reorder based off orm_molecule ordered list
                query = format_query(MoleculeORM, molecule_hash=hash_list)
                indices = session.query(MoleculeORM.molecule_hash, MoleculeORM.id).filter(*query)

                id_map = {k: v for k, v in indices}
                n_inserted = len(orm_molecules)

            else:
                # Start from old ID map
                id_map = previous_id_map

                new_molecules = []
                n_inserted = 0

                for orm_mol in orm_molecules:
                    duplicate_id = id_map.get(orm_mol.molecule_hash, False)
                    if duplicate_id is not False:
                        meta["duplicates"].append(str(duplicate_id))
                    else:
                        new_molecules.append(orm_mol)
                        id_map[orm_mol.molecule_hash] = "placeholder_id"
                        n_inserted += 1
                        session.add(orm_mol)

                    # We should make sure there was not a hash collision?
                    # new_mol.compare(old_mol)
                    # raise KeyError("!!! WARNING !!!: Hash collision detected")

                session.commit()

                for new_mol in new_molecules:
                    id_map[new_mol.molecule_hash] = new_mol.id

            results = [str(id_map[x.molecule_hash]) for x in orm_molecules]
            assert "placeholder_id" not in results
            meta["n_inserted"] = n_inserted

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def get(self, id=None, molecule_hash=None, molecular_formula=None, limit: int = None, skip: int = 0):
        try:
            if isinstance(molecular_formula, str):
                molecular_formula = qcelemental.molutil.order_molecular_formula(molecular_formula)
            elif isinstance(molecular_formula, list):
                molecular_formula = [qcelemental.molutil.order_molecular_formula(form) for form in molecular_formula]
        except ValueError:
            # Probably, the user provided an invalid chemical formula
            pass

        meta = get_metadata_template()

        query = format_query(MoleculeORM, id=id, molecule_hash=molecule_hash, molecular_formula=molecular_formula)

        # Don't include the hash or the molecular_formula in the returned result
        rdata, meta["n_found"] = self._core_socket.get_query_projection(
            MoleculeORM, query, limit=limit, skip=skip, exclude=["molecule_hash", "molecular_formula"]
        )

        meta["success"] = True

        # This is required for sparse molecules as we don't know which values are spase
        # We are lucky that None is the default and doesn't mean anything in Molecule
        # This strategy does not work for other objects
        data = []
        for mol_dict in rdata:
            mol_dict = {k: v for k, v in mol_dict.items() if v is not None}
            data.append(Molecule(**mol_dict, validate=False, validated=True))

        return {"meta": meta, "data": data}

    def delete(self, id: List[str] = None, molecule_hash: List[str] = None):
        """
        Removes a molecule from the database from its hash.

        Parameters
        ----------
        id : str or List[str], optional
            ids of molecules, can use the hash parameter instead
        molecule_hash : str or List[str]
            The hash of a molecule.

        Returns
        -------
        bool
            Number of deleted molecules.
        """

        query = format_query(MoleculeORM, id=id, molecule_hash=molecule_hash)

        with self._core_socket.session_scope() as session:
            ret = session.query(MoleculeORM).filter(*query).delete(synchronize_session=False)

        return ret
