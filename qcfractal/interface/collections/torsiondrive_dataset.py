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
from ..visualization import custom_plot


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
            The requested specification.

        """
        try:
            return self.data.td_specs[name.lower()].copy()
        except KeyError:
            raise KeyError(f"TorsionDriveSpecification '{name}' not found.")

    def list_specifications(self, description=True) -> Union[List[str], 'DataFrame']:
        """Lists all available specifications

        Parameters
        ----------
        description : bool, optional
            If True returns a DataFrame with
            Description

        Returns
        -------
        Union[List[str], 'DataFrame']
            A list of known specification names.

        """
        if description:
            data = [(x.name, x.description) for x in self.data.td_specs.values()]
            return pd.DataFrame(data, columns=["Name", "Description"]).set_index("Name")
        else:
            return [x.name for x in self.data.td_specs.values()]

    def add_entry(self,
                  name: str,
                  initial_molecules: List[Molecule],
                  dihedrals: List[Tuple[int, int, int, int]],
                  grid_spacing: List[int],
                  dihedral_ranges: Optional[List[Tuple[int, int]]]=None,
                  energy_decrease_thresh: Optional[float]=None,
                  energy_upper_limit: Optional[float]=None,
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
        dihedral_ranges: Optional[List[Tuple[int, int]]]
            The range limit of each dihedrals to scan, within [-180, 360]
        energy_decrease_thresh: Optional[float]
            The threshold of energy decrease to trigger activating grid points
        energy_upper_limit: Optional[float]
            The upper limit of energy relative to current global minimum to trigger activating grid points
        attributes : Dict[str, Any], optional
            Additional attributes and descriptions for the record
        """

        # Build new objects
        molecule_ids = self.client.add_molecules(initial_molecules)
        td_keywords = TDKeywords(dihedrals=dihedrals, grid_spacing=grid_spacing, dihedral_ranges=dihedral_ranges,
            energy_decrease_thresh=energy_decrease_thresh, energy_upper_limit=energy_upper_limit)

        record = TDRecord(name=name, initial_molecules=molecule_ids, td_keywords=td_keywords, attributes=attributes)

        lname = name.lower()
        if lname in self.data.records:
            raise KeyError(f"Record {name} already in the dataset.")

        self.data.records[lname] = record
        self.save()

    def get_entry(self, name: str) -> TDRecord:
        """Obtains a record from the Dataset

        Parameters
        ----------
        name : str
            The record name to pull from.

        Returns
        -------
        TDRecord
            The requested record
        """
        return self.data.records[name.lower()]

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

        if not force and (spec.name in self.df):
            return spec.name

        query_ids = []
        mapper = {}
        for rec in self.data.records.values():
            try:
                td_id = rec.torsiondrives[spec.name]
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

        return spec.name

    def status(self, specs: Union[str, List[str]]=None, collapse: bool=True,
               status: Optional[str]=None) -> 'DataFrame':
        """Returns the status of all current specifications.

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

        # Specifications
        if isinstance(specs, str):
            specs = [specs]

        # Query all of the specs and make sure they are valid
        if specs is None:
            list_specs = list(self.df.columns)
        else:
            list_specs = []
            for spec in specs:
                list_specs.append(self.query(spec))

        # apply status by column then by row
        df = self.df[list_specs].apply(lambda col: col.apply(lambda entry: entry.status.value))
        if status:
            df = df[(df == status.upper()).all(axis=1)]

        if collapse:
            return df.apply(lambda x: x.value_counts())
        else:
            return df

    def counts(self,
               entries: Union[str, List[str]],
               specs: Optional[Union[str, List[str]]]=None,
               count_gradients=False) -> 'DataFrame':
        """Counts the number of optimization or gradient evaluations associated with the
        TorsionDrives.

        Parameters
        ----------
        entries : Union[str, List[str]]
            The entries to query for
        specs : Optional[Union[str, List[str]]], optional
            The specifications to query for
        count_gradients : bool, optional
            If True, counts the total number of gradient calls. Warning! This can be slow for large datasets.

        Returns
        -------
        DataFrame
            The queried counts.
        """

        # Specifications
        if isinstance(specs, str):
            specs = [specs]

        if isinstance(entries, str):
            entries = [entries]

        # Query all of the specs and make sure they are valid
        if specs is None:
            specs = list(self.df.columns)
        else:
            for spec in specs:
                self.query(spec)

        # Count functions
        def count_gradient_evals(td):
            if td.status != "COMPLETE":
                return None

            total_grads = 0
            for key, optimizations in td.get_history().items():
                for opt in optimizations:
                    total_grads += len(opt.trajectory)
            return total_grads

        def count_optimizations(td):
            if td.status != "COMPLETE":
                return None
            return sum(len(v) for v in td.optimization_history.values())

        # Loop over the data and apply the count function
        ret = []
        for col in specs:
            data = self.df[col]
            if entries:
                data = data[entries]

            if count_gradients:
                cnts = data.apply(lambda td: count_gradient_evals(td))
            else:
                cnts = data.apply(lambda td: count_optimizations(td))
            ret.append(cnts)

        ret = pd.DataFrame(ret).transpose()
        ret.dropna(inplace=True, how="all")
        # ret = pd.DataFrame([ret[x].astype(int) for x in ret.columns]).transpose()
        return ret

    def visualize(self,
                  entries: Union[str, List[str]],
                  specs: Union[str, List[str]],
                  relative: bool=True,
                  units: str="kcal / mol",
                  digits: int=3,
                  use_measured_angle: bool=False,
                  return_figure: Optional[bool]=None) -> 'plotly.Figure':
        """
        Parameters
        ----------
        entries : Union[str, List[str]]
            A single or list of indices to plot.
        specs : Union[str, List[str]]
            A single or list of specifications to plot.
        relative : bool, optional
            Shows relative energy, lowest energy per scan is zero.
        units : str, optional
            The units of the plot.
        digits : int, optional
            Rounds the energies to n decimal places for display.
        use_measured_angle : bool, optional
            If True, the measured final angle instead of the constrained optimization angle.
            Can provide more accurate results if the optimization was ill-behaved,
            but pulls additional data from the server and may take longer.
        return_figure : Optional[bool], optional
            If True, return the raw plotly figure. If False, returns a hosted iPlot. If None, return a iPlot display in Jupyter notebook and a raw plotly figure in all other circumstances.

        Returns
        -------
        plotly.Figure
            The requested figure.
        """

        show_spec = True
        if isinstance(specs, str):
            specs = [specs]
            show_spec = False

        if isinstance(entries, str):
            entries = [entries]

        # Query all of the specs and make sure they are valid
        for spec in specs:
            self.query(spec)

        traces = []
        ranges = []
        # Loop over specifications
        for spec in specs:
            # Loop over indices (groups colors by entry)
            for index in entries:

                # Plot the figure using the torsiondrives plotting function
                fig = self.df.loc[index, spec].visualize(
                    relative=relative,
                    units=units,
                    digits=digits,
                    use_measured_angle=use_measured_angle,
                    return_figure=True)

                ranges.append(fig.layout.xaxis.range)
                trace = fig.data[0] # Pull out the underlying scatterplot

                if show_spec:
                    trace.name = f"{index}-{spec}"
                else:
                    trace.name = f"{index}"

                traces.append(trace)

        title = "TorsionDriveDataset 1-D Plot"
        if show_spec is False:
            title += f" [spec={specs[0]}]"

        if relative:
            ylabel = f"Relative Energy [{units}]"
        else:
            ylabel = f"Absolute Energy [{units}]"

        custom_layout = {
            "title": title,
            "yaxis": {
                "title": ylabel,
                "zeroline": True
            },
            "xaxis": {
                "title": "Dihedral Angle [degrees]",
                "zeroline": False,
                "range": [min(x[0] for x in ranges), max(x[1] for x in ranges)]
            }
        }

        return custom_plot(traces, custom_layout, return_figure=return_figure)


register_collection(TorsionDriveDataset)
