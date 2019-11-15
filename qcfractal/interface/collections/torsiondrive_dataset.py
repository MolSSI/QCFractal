"""
QCPortal Database ODM
"""
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd

from ..models import ObjectId, OptimizationSpecification, ProtoModel, QCSpecification, TorsionDriveInput
from ..models.torsiondrive import TDKeywords
from ..visualization import custom_plot
from .collection import BaseProcedureDataset
from .collection_utils import register_collection

if TYPE_CHECKING:  # pragma: no cover
    from ..models import Molecule


class TDEntry(ProtoModel):
    """Data model for the `reactions` list in Dataset"""

    name: str
    initial_molecules: Set[ObjectId]
    td_keywords: TDKeywords
    attributes: Dict[str, Any]
    object_map: Dict[str, ObjectId] = {}


class TDEntrySpecification(ProtoModel):
    name: str
    description: Optional[str]
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification


class TorsionDriveDataset(BaseProcedureDataset):
    class DataModel(BaseProcedureDataset.DataModel):

        records: Dict[str, TDEntry] = {}
        history: Set[str] = set()
        specs: Dict[str, TDEntrySpecification] = {}

        class Config(BaseProcedureDataset.DataModel.Config):
            pass

    def _internal_compute_add(self, spec: Any, entry: Any, tag: str, priority: str) -> ObjectId:

        service = TorsionDriveInput(
            initial_molecule=entry.initial_molecules,
            keywords=entry.td_keywords,
            optimization_spec=spec.optimization_spec,
            qc_spec=spec.qc_spec,
        )

        return self.client.add_service([service], tag=tag, priority=priority).ids[0]

    def add_specification(
        self,
        name: str,
        optimization_spec: OptimizationSpecification,
        qc_spec: QCSpecification,
        description: Optional[str] = None,
        overwrite: bool = False,
    ) -> None:
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

        spec = TDEntrySpecification(
            name=name, optimization_spec=optimization_spec, qc_spec=qc_spec, description=description
        )

        return self._add_specification(name, spec, overwrite=overwrite)

    def add_entry(
        self,
        name: str,
        initial_molecules: List["Molecule"],
        dihedrals: List[Tuple[int, int, int, int]],
        grid_spacing: List[int],
        dihedral_ranges: Optional[List[Tuple[int, int]]] = None,
        energy_decrease_thresh: Optional[float] = None,
        energy_upper_limit: Optional[float] = None,
        attributes: Dict[str, Any] = None,
        save: bool = True,
    ) -> None:
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
            Additional attributes and descriptions for the entry
        save : bool, optional
            If true, saves the collection after adding the entry. If this is False be careful
            to call save after all entries are added, otherwise data pointers may be lost.
        """

        self._check_entry_exists(name)  # Fast skip

        if attributes is None:
            attributes = {}

        # Build new objects
        molecule_ids = self.client.add_molecules(initial_molecules)
        td_keywords = TDKeywords(
            dihedrals=dihedrals,
            grid_spacing=grid_spacing,
            dihedral_ranges=dihedral_ranges,
            energy_decrease_thresh=energy_decrease_thresh,
            energy_upper_limit=energy_upper_limit,
        )

        entry = TDEntry(name=name, initial_molecules=molecule_ids, td_keywords=td_keywords, attributes=attributes)

        self._add_entry(name, entry, save)

    def counts(
        self,
        entries: Union[str, List[str]],
        specs: Optional[Union[str, List[str]]] = None,
        count_gradients: bool = False,
    ) -> pd.DataFrame:
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

        if isinstance(specs, str):
            specs = [specs]

        if isinstance(entries, str):
            entries = [entries]

        # Query all of the specs and make sure they are valid
        if specs is None:
            specs = list(self.df.columns)
        else:
            new_specs = []
            for spec in specs:
                new_specs.append(self.query(spec))

            # Remap names
            specs = new_specs

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

    def visualize(
        self,
        entries: Union[str, List[str]],
        specs: Union[str, List[str]],
        relative: bool = True,
        units: str = "kcal / mol",
        digits: int = 3,
        use_measured_angle: bool = False,
        return_figure: Optional[bool] = None,
    ) -> "plotly.Figure":
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
        formatted_spec_names = []
        for spec in specs:
            formatted_spec_names.append(self.query(spec))

        traces = []
        ranges = []
        # Loop over specifications
        for spec in formatted_spec_names:
            # Loop over indices (groups colors by entry)
            for index in entries:

                # Plot the figure using the torsiondrives plotting function
                fig = self.df.loc[index, spec].visualize(
                    relative=relative,
                    units=units,
                    digits=digits,
                    use_measured_angle=use_measured_angle,
                    return_figure=True,
                )

                ranges.append(fig.layout.xaxis.range)
                trace = fig.data[0]  # Pull out the underlying scatterplot

                if show_spec:
                    trace.name = f"{index}-{spec}"
                else:
                    trace.name = f"{index}"

                traces.append(trace)

        title = "TorsionDriveDataset 1-D Plot"
        if show_spec is False:
            title += f" [spec={formatted_spec_names[0]}]"

        if relative:
            ylabel = f"Relative Energy [{units}]"
        else:
            ylabel = f"Absolute Energy [{units}]"

        custom_layout = {
            "title": title,
            "yaxis": {"title": ylabel, "zeroline": True},
            "xaxis": {
                "title": "Dihedral Angle [degrees]",
                "zeroline": False,
                "range": [min(x[0] for x in ranges), max(x[1] for x in ranges)],
            },
        }

        return custom_plot(traces, custom_layout, return_figure=return_figure)


register_collection(TorsionDriveDataset)
