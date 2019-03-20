"""
QCPortal Database ODM
"""
import itertools as it
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd

from pydantic import BaseModel
from qcelemental import constants

from .collection_utils import nCr, register_collection
from .collection import Collection
from ..dict_utils import replace_dict_keys
from ..models import ObjectId, Molecule, OptimizationSpecification, QCSpecification
from ..models.torsiondrive import TDKeywords


class TDRecord(BaseModel):
    """Data model for the `reactions` list in Dataset"""
    name: str
    initial_molecules: List[ObjectId]
    td_keywords: TDKeywords
    attributes: Dict[str, Union[int, float, str]]  # Might be overloaded key types


class TorsionDriveSpecification(BaseModel):
    name: str
    description: Optional[str]
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification


class TorsionDriveDataset(Collection):
    def __init__(self, name: str, client: 'FractalClient'=None, **kwargs):
        if client is None:
            raise KeyError("TorsionDriveDataset must have a client.")

        super().__init__(name, client=client, **kwargs)

    class DataModel(BaseModel):

        records: List[TDRecord] = []

        history: Set[Tuple[str]] = set()
        history_keys: Tuple[str] = ("torsiondrive_specification", )

        td_specs = Dict[str, TorsionDriveSpecification] = {}

    def _pre_save_prep(self, client: 'FractalClient') -> None:
        pass

    def add_specification(self,
                          name,
                          optimization_spec: OptimizationSpecification,
                          qc_spec: QCSpecification,
                          description: str=None,
                          overwrite=False) -> None:
        """
        Parameters
        ----------
        name : TYPE
            The name of the specification
        optimization_spec : OptimizationSpecification
            A full optimization specification for TorsionDrive
        qc_spec : QCSpecification
            A full quantum chemistry specification for TorsionDrive
        description : str, optional
            A short text description of the specification
        overwrite : bool, optional
            Overwrite existing specification names

        """

        lname = name.lower()
        if (lname in td_specs) and (not overwrite):
            raise KeyError(f"TorsionDriveSpecification '{name}' already present, use `overwrite=True` to replace.")

        spec = TorsionDriveSpecification(
            name=lname, optimization_spec=optimization_spec, qc_spec=qc_spec, description=description)
        self.data.td_specs.append(spec)

    def get_specification(self, name:str) -> TorsionDriveSpecification:
        """
        Parameters
        ----------
        name : str
            The name of the specification

        Returns
        -------
        TorsionDriveSpecification
            The requestion specification.

        """
        try:
            return self.data.td_specs[name.lower()].copy()
        except KeyError:
            raise KeyError(f"TorsionDriveSpecification '{name}' not found.")

    def list_specifications(self) -> List[str]:
        """Lists all available specifications known

        Returns
        -------
        List[str]
            A list of known specification names.
        """
        return list(self.data.td_specs)

    def add_entry(self,
                  name: str,
                  initial_molecules: List[Molecule],
                  dihedrals: List[Tuple[int, int, int, int]],
                  grid_spacing: List[int],
                  attributes: Dict[str, Any]=None):
        """
        Parameters
        ----------
        name : str
            The name of the entry, will be used for the index
        initial_molecules : List[Molecule]
            The list of starting Molecules for the TorsionDrive
        dihedrals : List[Tuple[int, int, int, int]]
            A list of dihedrals to scan over
        grid_spacing : List[int]
            The grid spacing for each dihedrals
        attributes : Dict[str, Any], optional
            Additional attributes and descriptions for the record
        """

        # Build new objects
        molecule_ids = self.client.add_molecules(initial_molecules)
        td_keywords = TDKeywords(dihedrals=dihedrals, grid_spacing=grid_spacing)

        record = TDRecord(name=name, initial_molecules=molecule_ids, td_keywords=td_keywords, attributes=attributes)

        self.data.records.append(record)


register_collection(TorsionDriveDataset)
