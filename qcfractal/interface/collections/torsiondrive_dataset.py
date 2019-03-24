"""
QCPortal Database ODM
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd

from pydantic import BaseModel

from .collection_utils import register_collection
from .collection import Collection
from ..models import ObjectId, Molecule, OptimizationSpecification, QCSpecification, TorsionDriveInput
from ..models.torsiondrive import TDKeywords


class TDRecord(BaseModel):
    """Data model for the `reactions` list in Dataset"""
    name: str
    initial_molecules: List[ObjectId]
    td_keywords: TDKeywords
    attributes: Dict[str, Union[int, float, str]]  # Might be overloaded key types
    torsiondrives: Dict[str, ObjectId] = {}


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

        self.df = pd.DataFrame(index=self._get_index())

    class DataModel(Collection.DataModel):

        records: Dict[str, TDRecord] = {}
        history: Set[str] = set()
        td_specs: Dict[str, TorsionDriveSpecification] = {}

    def _pre_save_prep(self, client: 'FractalClient') -> None:
        pass

    def _get_index(self):

        return [x.name for x in self.data.records.values()]

    def add_specification(self,
                          name: str,
                          optimization_spec: OptimizationSpecification,
                          qc_spec: QCSpecification,
                          description: str=None,
                          overwrite=False) -> None:
        """
        Parameters
        ----------
        name : str
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
        if (lname in self.data.td_specs) and (not overwrite):
            raise KeyError(f"TorsionDriveSpecification '{name}' already present, use `overwrite=True` to replace.")

        spec = TorsionDriveSpecification(
            name=lname, optimization_spec=optimization_spec, qc_spec=qc_spec, description=description)
        self.data.td_specs[lname] = spec
        self.save()

    def get_specification(self, name: str) -> TorsionDriveSpecification:
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
        return [x.name for x in self.data.td_specs]

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

        lname = name.lower()
        if lname in self.data.records:
            raise KeyError(f"Record {name} already in the dataset.")

        self.data.records[lname] = record
        self.save()

    def compute(self, specification: str, subset: Set[str]=None, tag: Optional[str]=None,
                priority: Optional[str]=None) -> int:
        """Computes a specification for all records in the dataset.

        Parameters
        ----------
        specification : str
            The specification name.
        subset : Set[str], optional
            Computes only a subset of the dataset.
        tag : Optional[str], optional
            The queue tag to use when submitting compute requests.
        priority : Optional[str], optional
            The priority of the jobs low, medium, or high.

        Returns
        -------
        int
            The number of submitted torsiondrives
        """
        specification = specification.lower()
        spec = self.get_specification(specification)
        if subset:
            subset = set(subset)

        submitted = 0
        for rec in self.data.records.values():
            if specification in rec.torsiondrives:
                continue

            if (subset is not None) and (rec.name not in subset):
                continue

            service = TorsionDriveInput(
                initial_molecule=rec.initial_molecules,
                keywords=rec.td_keywords,
                optimization_spec=spec.optimization_spec,
                qc_spec=spec.qc_spec)

            rec.torsiondrives[specification] = self.client.add_service([service], tag=tag, priority=priority).ids[0]
            submitted += 1

        self.data.history.add(specification)
        self.save()
        return submitted

    def query(self, specification: str, force: bool=False) -> None:
        """Queries a given specification from the server

        Parameters
        ----------
        specification : str
            The specification name to query
        force : bool, optional
            Force a fresh query if the specification already exists.
        """
        # Try to get the specification, will throw if not found.
        spec = self.get_specification(specification)

        if force and (spec.name in self.df):
            return spec.name

        spec_name = specification.lower()
        query_ids = []
        mapper = {}
        for rec in self.data.records.values():
            try:
                td_id = rec.torsiondrives[spec_name]
                query_ids.append(td_id)
                mapper[td_id] = rec.name
            except KeyError:
                pass

        torsiondrives = self.client.query_procedures(id=query_ids)

        data = []
        for td in torsiondrives:
            data.append([mapper[td.id], td])

        df = pd.DataFrame(data, columns=["index", spec.name])
        df.set_index("index", inplace=True)

        self.df[spec.name] = df[spec.name]

    def status(self, collapse: bool=True, status:Optional[str]=None) -> 'DataFrame':
        """Returns the current status of all current specifications.

        Parameters
        ----------
        collapse : bool, optional
            Collapse the status into summaries per specification or not.
        status : Optional[str], optional
            If not None, only returns results that match the provided status.

        Returns
        -------
        DataFrame
            A DataFrame of all known statuses

        """

        # apply status by column then by row
        df = self.df.apply(lambda col: col.apply(lambda entry: entry.status.value))
        if status:
            df = df[(df == status.upper()).all(axis=1)]

        if collapse:
            return df.apply(lambda x: x.value_counts())
        else:
            return df

    def visualize(self, entries: Union[str, List[str]], specs: Union[str, List[str]]) -> 'plotly.Figure':

        # Query all of the specs and make sure they are valid
        for x in specs:
            self.query(x)


register_collection(TorsionDriveDataset)
