"""
A model for TorsionDrive
"""

import copy
import json
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
from pydantic import BaseModel, constr, validator
from qcelemental import constants

from .common_models import Molecule, ObjectId, OptimizationSpecification, QCSpecification
from .model_utils import json_encoders, recursive_normalizer
from .records import RecordBase
from ..visualization import scatter_plot

__all__ = ["TorsionDriveInput", "TorsionDriveRecord"]


class TDKeywords(BaseModel):
    """
    TorsionDriveRecord options
    """
    dihedrals: List[Tuple[int, int, int, int]]
    grid_spacing: List[int]
    dihedral_ranges: Optional[List[Tuple[int, int]]] = None
    energy_decrease_thresh: Optional[float] = None
    energy_upper_limit: Optional[float] = None

    def __init__(self, **kwargs):
        super().__init__(**recursive_normalizer(kwargs))

    class Config:
        extra = "allow"
        allow_mutation = False


_td_constr = constr(strip_whitespace=True, regex="torsiondrive")
_qcfractal_constr = constr(strip_whitespace=True, regex="qcfractal")


class TorsionDriveInput(BaseModel):
    """
    A TorsionDriveRecord Input base class
    """

    program: _td_constr = "torsiondrive"
    procedure: _td_constr = "torsiondrive"
    initial_molecule: List[Union[ObjectId, Molecule]]
    keywords: TDKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    @validator('initial_molecule', pre=True, whole=True)
    def check_initial_molecules(cls, v):
        if isinstance(v, (str, dict, Molecule)):
            v = [v]
        return v

    class Config:
        extras = "forbid"
        allow_mutation = False
        json_encoders = json_encoders


class TorsionDriveRecord(RecordBase):
    """
    A interface to the raw JSON data of a TorsionDriveRecord torsion scan run.
    """

    # Classdata
    _hash_indices = {"initial_molecule", "keywords", "optimization_spec", "qc_spec"}

    # Version data
    version: int = 1
    procedure: _td_constr = "torsiondrive"
    program: _td_constr = "torsiondrive"

    # Input data
    initial_molecule: List[ObjectId]
    keywords: TDKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    # Output data
    final_energy_dict: Dict[str, float]

    optimization_history: Dict[str, List[ObjectId]]
    minimum_positions: Dict[str, int]

    class Config(RecordBase.Config):
        pass

## Utility

    def _serialize_key(self, key):
        if isinstance(key, str):
            return key
        elif isinstance(key, (int, float)):
            key = (int(key), )

        return json.dumps(key)

    def _deserialize_key(self, key: str) -> Tuple[int, ...]:
        return tuple(json.loads(key))

    def _organize_return(self, data: Dict[str, Any], key: Union[int, str, None],
                         minimum: bool=False) -> Dict[str, Any]:

        if key is None:
            return {self._deserialize_key(k): copy.deepcopy(v) for k, v in data.items()}

        key = self._serialize_key(key)

        if minimum:
            minpos = self.minimum_positions[self._serialize_key(key)]
            return copy.deepcopy(data[key][minpos])
        else:
            return copy.deepcopy(data[key])

## Query

    def get_history(self, key: Union[int, Tuple[int, ...], str]=None, minimum: bool=False) -> Dict[str, List['ResultRecord']]:
        """Queries the server for all optimization trajectories.

        Parameters
        ----------
        key : Union[int, Tuple[int, ...], str], optional
            Specifies a single entry to pull from.
        minimum : bool, optional
            If true only returns the minimum optimization, otherwise returns all trajectories.

        Returns
        -------
        Dict[str, List['ResultRecord']]
            The optimization history

        """

        if "history" not in self.cache:

            # Grab procedures
            needed_ids = [x for v in self.optimization_history.values() for x in v]
            objects = self.client.query_procedures(id=needed_ids)
            procedures = {v.id: v for v in objects}

            # Move procedures into the correct order
            ret = {}
            for okey, hashes in self.optimization_history.items():
                tmp = []
                for h in hashes:
                    tmp.append(procedures[h])
                ret[okey] = tmp

            self.cache["history"] = ret

        data = self.cache["history"]

        return self._organize_return(data, key, minimum=minimum)


    def get_final_energies(self, key: Union[int, Tuple[int, ...], str]=None) -> Dict[str, float]:
        """
        Provides the final optimized energies at each grid point.

        Parameters
        ----------
        key : Union[int, Tuple[int, ...], str], optional
            Specifies a single entry to pull from.

        Returns
        -------
        energy : Dict[str, float]
            Returns energies at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------
        >>> torsiondrive_obj.get_final_energies()
        {(-90,): -148.7641654446243, (180,): -148.76501336993732, (0,): -148.75056290106735, (90,): -148.7641654446148}

        """

        return self._organize_return(self.final_energy_dict, key)


    def get_final_molecules(self, key: Union[int, Tuple[int, ...], str]=None) -> Dict[str, 'Molecule']:
        """Returns the optimized molecules at each grid point

        Parameters
        ----------
        key : Union[int, Tuple[int, ...], str], optional
            Specifies a single entry to pull from.

        Returns
        -------
        final_molecules : Dict[str, 'Molecule']
            Returns molecule at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------
        >>> mols = torsiondrive_obj.get_final_molecules()
        >>> type(mols[(-90, )])
        qcelemental.models.molecule.Molecule

        >>> type(torsiondrive_obj.get_final_molecules((-90,)))
        qcelemental.models.molecule.Molecule

        """

        if "final_molecules" not in self.cache:

            ret = {}
            for k, tasks in self.get_history().items():
                k = self._serialize_key(k)
                minpos = self.minimum_positions[k]

                ret[k] = tasks[minpos].get_final_molecule()

            self.cache["final_molecules"] = ret

        data = self.cache["final_molecules"]

        return self._organize_return(data, key)


    def get_final_results(self, key: Union[int, Tuple[int, ...], str]=None) -> Dict[str, 'ResultRecord']:
        """Returns the final opt gradient result records at each grid point

        Parameters
        ----------
        key : Union[int, Tuple[int, ...], str], optional
            Specifies a single entry to pull from.

        Returns
        -------
        final_results : Dict[str, 'ResultRecord']
            Returns ResultRecord at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------
        >>> mols = torsiondrive_obj.get_final_results()
        >>> type(mols[(-90, )])
        qcfractal.interface.models.records.ResultRecord

        >>> type(torsiondrive_obj.get_final_results((-90,)))
        qcfractal.interface.models.records.ResultRecord

        """

        if "final_results" not in self.cache:

            map_id_key = {}
            ret = {}
            for k, tasks in self.get_history().items():
                k = self._serialize_key(k)
                minpos = self.minimum_positions[k]
                final_opt_task = tasks[minpos]
                if len(final_opt_task.trajectory) > 0:
                    final_grad_record_id = final_opt_task.trajectory[-1]
                    # store the id -> grid id mapping
                    map_id_key[final_grad_record_id] = k
            # combine the ids into one query
            query_result_ids = list(map_id_key.keys())
            # run the query on this batch
            for grad_result_record in self.client.query_results(id=query_result_ids):
                k = map_id_key[grad_result_record.id]
                ret[k] = grad_result_record

            self.cache["final_results"] = ret

        data = self.cache["final_results"]

        return self._organize_return(data, key)

    def visualize(self,
                  relative: bool=True,
                  units: str="kcal / mol",
                  digits: int=3,
                  use_measured_angle: bool=False,
                  return_figure: Optional[bool]=None) -> 'plotly.Figure':
        """
        Parameters
        ----------
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

        # Pull energy dictionary apart
        min_energy = 1e12
        x = []
        y = []
        for k, v in self.get_final_energies().items():
            if len(k) >= 2:
                raise TypeError("TorsionDrive.visualize is currently only available for 1-D scans.")

            if use_measured_angle:
                # Recalculate the dihedral angle
                dihedral_indices = self.keywords.dihedrals[0]
                mol = self.get_final_molecules(k)
                x.append(mol.measure(dihedral_indices))

            else:
                x.append(k[0])

            y.append(v)

            # Update minmum energy
            if v < min_energy:
                min_energy = v

        x = np.array(x)
        y = np.array(y)

        # Sort by angle
        sorter = np.argsort(x)
        x = x[sorter]
        y = y[sorter]
        if relative:
            y -= min_energy

        cf = constants.conversion_factor("hartree", units)
        trace = {"mode": "lines+markers", "x": x, "y": np.around(y * cf, digits)}
        # "name": "something"

        title = "TorsionDrive 1-D Plot"

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
                "range": [x.min() - 10, x.max() + 10]
            }
        }

        return scatter_plot([trace], custom_layout=custom_layout, return_figure=return_figure)
