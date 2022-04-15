from typing import List, Optional, Tuple, Union, Dict

from pydantic import BaseModel, Field, Extra, root_validator, constr, validator
from typing_extensions import Literal

from .. import BaseRecord, RecordAddBodyBase, RecordQueryBody
from ..singlepoint.models import QCInputSpecification, QCSpecification, SinglepointRecord, SinglepointDriver
from ...base_models import ProjURLParameters
from ...molecules import Molecule
from ...utils import recursive_normalizer


class NEBKeywords(BaseModel):
    """
    NEBRecord options
    """

    images: int = Field(
        21,
        description="Number of images for the NEB method. Each image represents a specific geometry.",
        gt=5,
    )
 
    spring_constant: float = Field(
        1.0,
        description="Sping constant in kcal/mol/Ang^2.",
    )
        

    energy_weighted: bool = Field(
        False,
        description="Energy weighted NEB method varies the spring constant based on the image's energy.",
    )

    @root_validator
    def normalize(cls, values):
        return recursive_normalizer(values)

class NEBQCSpecification(QCInputSpecification):

    #id: int
    #qc_specification_id: int
    #qc_specification: QCSpecification
    driver: SinglepointDriver = SinglepointDriver.gradient

    @validator("driver", pre=True)
    def force_driver(cls, v):
        return SinglepointDriver.gradient

class NEBSpecification(BaseModel):
    program: constr(to_lower=True) = "geometric"
    qc_specification: NEBQCSpecification
    keywords: NEBKeywords
    
#class NEBSpecification(BaseModel):
#
#    qc_specification_id: int

#class NEBSpecificationID(NEBSpecification):
#    id: int
#    qc_specification_id:int
#    qc_specification : QCSpecification
    

class NEBSinglepoints(BaseModel):
    class Config:
        extra = Extra.forbid

    neb_id: int
    singlepoint_id: int
    iteration: int
    position: int

    #energy: Optional[float] = None
    singlepoint_record: Optional[SinglepointRecord._DataModel]


class NEBInitialchain(BaseModel):
    id: int
    molecule_id: int
    position: int

    molecule: Optional[Molecule]
    

class NEBAddBody(RecordAddBodyBase):
    specification: NEBSpecification
    initial_chain: List[List[Union[int, Molecule]]]


class NEBQueryBody(RecordQueryBody):
    program: Optional[List[str]] = None
    neb_program: Optional[List[str]]
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    qc_keywords_id: Optional[List[int]] = None
    initial_chain_id: Optional[List[int]] = None

    @validator("qc_basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


class NEBRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["neb"]
        specification_id: int
        specification: NEBSpecification
        initial_chain: Optional[List[NEBInitialchain]] = None
        singlepoints: Optional[List[NEBSinglepoints]] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["neb"]
    raw_data: _DataModel

    singlepoint_cache: Optional[Dict[str, SinglepointRecord]] = None

    def _retrieve_initial_chain(self):
        self.raw_data.initial_chain = self.client._auto_request(
            "get",
            f"v1/records/neb/{self.raw_data.id}/initial_chain",
            None,
            None,
            List[Molecule],
            None,
            None,
        )

    def _retrieve_sinlepoints(self):
        url_params = {"include": ["*", "singlepoint_record"]}

        self.raw_data.singlepoints = self.client._auto_request(
            "get",
            f"v1/records/neb/{self.raw_data.id}/singlepoints",
            None,
            ProjURLParameters,
            List[NEBSinglepoints],
            None,
            url_params,
        )

    @property
    def specification_id(self) -> int:
        return self.raw_data.specification_id

    @property
    def specification(self) -> NEBQCSpecification:
        return self.raw_data.specification

    @property
    def initial_chain(self) -> List[Molecule]:
        if self.raw_data.initial_chain is None:
            self._retrieve_initial_chain()
        return self.raw_data.initial_chain

    @property
    def singlepoints(self) -> Dict[str, SinglepointRecord]:
        if self.singlepoint_cache is not None:
            return self.singlepoint_cache

        # convert the raw singlepoint data to a dictionary of key -> SinglepointRecord
        if self.raw_data.singlepoints is None:
            self._retrieve_singlepoints()

        ret = {}
        for sing in self.raw_data.singlepoints:
            ret.setdefault(sing.key, list())
            ret[opt.key].append(self.client.recordmodel_from_datamodel([sing.singlepoint_record])[0])
        self.singlepoint_cache = ret
        return ret


# class TorsiondriveRecord(RecordBase):
#    """
#    A interface to the raw JSON data of a TorsionDriveRecord torsion scan run.
#    """
#
#    # Class data
#    _hash_indices = {"initial_molecule", "keywords", "optimization_spec", "qc_spec"}
#
#    # Version data
#    version: int = Field(1, description="The version number of the Record.")
#    procedure: _td_constr = Field(
#        "torsiondrive",
#        description="The name of the procedure. Fixed to 'torsiondrive' since this is the Record explicit to "
#                    "TorsionDrive.",
#    )
#    program: _td_constr = Field(
#        "torsiondrive",
#        description="The name of the program. Fixed to 'torsiondrive' since this is the Record explicit to "
#                    "TorsionDrive.",
#    )
#
#    # Input data
#    initial_molecule: List[ObjectId] = Field(..., description="Id(s) of the initial molecule(s) in the database.")
#    keywords: TorsiondriveKeywords = Field(
#        ..., description="The TorsionDrive-specific input arguments used for this operation."
#    )
#    optimization_spec: OptimizationSpecification = Field(
#        ...,
#        description="The settings which describe how the energy optimizations at each step of the torsion "
#                    "scan used for this operation.",
#    )
#    qc_spec: QCSpecification = Field(
#        ...,
#        description="The settings which describe how the individual quantum chemistry calculations are handled for "
#                    "this operation.",
#    )
#
#    # Output data
#    final_energy_dict: Dict[str, float] = Field(
#        ..., description="The final energy at each angle of the TorsionDrive scan."
#    )
#
#    optimization_history: Dict[str, List[ObjectId]] = Field(
#        ...,
#        description="The map of each angle of the TorsionDrive scan to each optimization computations. "
#                    "Each value of the dict maps to a sequence of :class:`ObjectId` strings which each "
#                    "point to a single computation in the Database.",
#    )
#    minimum_positions: Dict[str, int] = Field(  # TODO: This could use review
#        ...,
#        description="A map of each TorsionDrive angle to the integer index of that angle's optimization "
#                    "trajectory which has the minimum-energy of the trajectory.",
#    )
#
#    class Config(RecordBase.Config):
#        pass
#
#    ## Utility
#
#    def _serialize_key(self, key):
#        if isinstance(key, str):
#            return key
#        elif isinstance(key, (int, float)):
#            key = (int(key),)
#
#        return json.dumps(key)
#
#    def _deserialize_key(self, key: str) -> Tuple[int, ...]:
#        return tuple(json.loads(key))
#
#    def _organize_return(
#            self, data: Dict[str, Any], key: Union[int, str, None], minimum: bool = False
#    ) -> Dict[str, Any]:
#
#        if key is None:
#            return {self._deserialize_key(k): copy.deepcopy(v) for k, v in data.items()}
#
#        key = self._serialize_key(key)
#
#        if minimum:
#            minpos = self.minimum_positions[self._serialize_key(key)]
#            return copy.deepcopy(data[key][minpos])
#        else:
#            return copy.deepcopy(data[key])
#
#    def detailed_status(self) -> Dict[str, Any]:
#
#        # Compute the total number of grid points
#        tpoints = 1
#        for x in self.keywords.grid_spacing:
#            tpoints *= int(360 / x)
#
#        flat_history = [x for v in self.get_history().values() for x in v]
#
#        ret = {
#            "status": self.status.value,
#            "total_points": tpoints,
#            "computed_points": len(self.optimization_history),
#            "complete_tasks": sum(x.status == RecordStatusEnum.complete for x in flat_history),
#            "incomplete_tasks": sum(
#                (x.status == RecordStatusEnum.waiting) or (x.status == RecordStatusEnum.running) for x in flat_history
#            ),
#            "error_tasks": sum(x.status == RecordStatusEnum.error for x in flat_history),
#        }
#        ret["current_tasks"] = ret["error_tasks"] + ret["incomplete_tasks"]
#        ret["percent_complete"] = ret["computed_points"] / ret["total_points"] * 100
#        ret["errors"] = [x for x in flat_history if x.status == RecordStatusEnum.error]
#
#        return ret
#
#    ## Query
#
#    def get_history(
#            self, key: Union[int, Tuple[int, ...], str] = None, minimum: bool = False
#    ) -> Dict[str, List["ResultRecord"]]:
#        """Queries the server for all optimization trajectories.
#
#        Parameters
#        ----------
#        key : Union[int, Tuple[int, ...], str], optional
#            Specifies a single entry to pull from.
#        minimum : bool, optional
#            If true only returns the minimum optimization, otherwise returns all trajectories.
#
#        Returns
#        -------
#        Dict[str, List['ResultRecord']]
#            The optimization history
#
#        """
#
#        if "history" not in self.cache:
#
#            # Grab procedures
#            needed_ids = [x for v in self.optimization_history.values() for x in v]
#            objects = []
#            query_limit = self.client.query_limits["record"]
#            for i in range(0, len(needed_ids), query_limit):
#                objects.extend(self.client.query_procedures(id=needed_ids[i : i + query_limit]))
#            procedures = {v.id: v for v in objects}
#
#            # Move procedures into the correct order
#            ret = {}
#            for okey, hashes in self.optimization_history.items():
#                tmp = []
#                for h in hashes:
#                    tmp.append(procedures[h])
#                ret[okey] = tmp
#
#            self.cache["history"] = ret
#
#        data = self.cache["history"]
#
#        return self._organize_return(data, key, minimum=minimum)
#
#    def get_final_energies(self, key: Union[int, Tuple[int, ...], str] = None) -> Dict[str, float]:
#        """
#        Provides the final optimized energies at each grid point.
#
#        Parameters
#        ----------
#        key : Union[int, Tuple[int, ...], str], optional
#            Specifies a single entry to pull from.
#
#        Returns
#        -------
#        energy : Dict[str, float]
#            Returns energies at each grid point in a dictionary or at a
#            single point if a key is specified.
#
#        Examples
#        --------
#        >>> torsiondrive_obj.get_final_energies()
#        {(-90,): -148.7641654446243, (180,): -148.76501336993732, (0,): -148.75056290106735, (90,): -148.7641654446148}
#
#        """
#
#        return self._organize_return(self.final_energy_dict, key)
#
#    def get_final_molecules(self, key: Union[int, Tuple[int, ...], str] = None) -> Dict[str, "Molecule"]:
#        """Returns the optimized molecules at each grid point
#
#        Parameters
#        ----------
#        key : Union[int, Tuple[int, ...], str], optional
#            Specifies a single entry to pull from.
#
#        Returns
#        -------
#        final_molecules : Dict[str, 'Molecule']
#            Returns molecule at each grid point in a dictionary or at a
#            single point if a key is specified.
#
#        Examples
#        --------
#        >>> mols = torsiondrive_obj.get_final_molecules()
#        >>> type(mols[(-90, )])
#        qcelemental.models.molecule.Molecule
#
#        >>> type(torsiondrive_obj.get_final_molecules((-90,)))
#        qcelemental.models.molecule.Molecule
#
#        """
#
#        if "final_molecules" not in self.cache:
#
#            map_id_key = self._get_min_optimization_map()
#
#            opt_ids = list(map_id_key.keys())
#
#            procs = self.client.query_optimizations(id=opt_ids, include=["id", "final_molecule_obj"])
#
#            ret = {map_id_key[p["id"]]: Molecule(**p["final_molecule_obj"]) for p in procs}
#
#            self.cache["final_molecules"] = ret
#
#        data = self.cache["final_molecules"]
#
#        return self._organize_return(data, key)
#
#    def get_final_results(self, key: Union[int, Tuple[int, ...], str] = None) -> Dict[str, "ResultRecord"]:
#        """Returns the final opt gradient result records at each grid point
#
#        Parameters
#        ----------
#        key : Union[int, Tuple[int, ...], str], optional
#            Specifies a single entry to pull from.
#
#        Returns
#        -------
#        final_results : Dict[str, 'ResultRecord']
#            Returns ResultRecord at each grid point in a dictionary or at a
#            single point if a key is specified.
#
#        Examples
#        --------
#        >>> mols = torsiondrive_obj.get_final_results()
#        >>> type(mols[(-90, )])
#        qcfractal.interface.models.records.ResultRecord
#
#        >>> type(torsiondrive_obj.get_final_results((-90,)))
#        qcfractal.interface.models.records.ResultRecord
#
#        """
#
#        if "final_results" not in self.cache:
#
#            map_id_key = self._get_min_optimization_map()
#            ret = {}
#
#            # combine the ids into one query
#            opt_ids = list(map_id_key.keys())
#
#            # TODO - custom endpoint for this again
#            results = self.client.query_optimizations(opt_ids, include=["*", "trajectory_obj"])
#
#            for opt in results:
#                k = map_id_key[opt["id"]]
#                ret[k] = opt["trajectory_obj"][-1]
#
#            self.cache["final_results"] = ret
#
#        data = self.cache["final_results"]
#
#        return self._organize_return(data, key)
#
#    def visualize(
#            self,
#            relative: bool = True,
#            units: str = "kcal / mol",
#            digits: int = 3,
#            use_measured_angle: bool = False,
#            return_figure: Optional[bool] = None,
#    ) -> "plotly.Figure":
#        """
#        Parameters
#        ----------
#        relative : bool, optional
#            Shows relative energy, lowest energy per scan is zero.
#        units : str, optional
#            The units of the plot.
#        digits : int, optional
#            Rounds the energies to n decimal places for display.
#        use_measured_angle : bool, optional
#            If True, the measured final angle instead of the constrained optimization angle.
#            Can provide more accurate results if the optimization was ill-behaved,
#            but pulls additional data from the server and may take longer.
#        return_figure : Optional[bool], optional
#            If True, return the raw plotly figure. If False, returns a hosted iPlot. If None, return a iPlot display in Jupyter notebook and a raw plotly figure in all other circumstances.
#
#        Returns
#        -------
#        plotly.Figure
#            The requested figure.
#        """
#
#        # Pull energy dictionary apart
#        min_energy = 1e12
#        x = []
#        y = []
#        for k, v in self.get_final_energies().items():
#            if len(k) >= 2:
#                raise TypeError("TorsionDrive.visualize is currently only available for 1-D scans.")
#
#            if use_measured_angle:
#                # Recalculate the dihedral angle
#                dihedral_indices = self.keywords.dihedrals[0]
#                mol = self.get_final_molecules(k)
#                x.append(mol.measure(dihedral_indices))
#
#            else:
#                x.append(k[0])
#
#            y.append(v)
#
#            # Update minimum energy
#            if v < min_energy:
#                min_energy = v
#
#        x = np.array(x)
#        y = np.array(y)
#
#        # Sort by angle
#        sorter = np.argsort(x)
#        x = x[sorter]
#        y = y[sorter]
#        if relative:
#            y -= min_energy
#
#        cf = constants.conversion_factor("hartree", units)
#        trace = {"mode": "lines+markers", "x": x, "y": np.around(y * cf, digits)}
#        # "name": "something"
#
#        title = "TorsionDrive 1-D Plot"
#
#        if relative:
#            ylabel = f"Relative Energy [{units}]"
#        else:
#            ylabel = f"Absolute Energy [{units}]"
#
#        custom_layout = {
#            "title": title,
#            "yaxis": {"title": ylabel, "zeroline": True},
#            "xaxis": {"title": "Dihedral Angle [degrees]", "zeroline": False, "range": [x.min() - 10, x.max() + 10]},
#        }
#
#        return scatter_plot([trace], custom_layout=custom_layout, return_figure=return_figure)
#
#    def _get_min_optimization_map(self):
#
#        map_id_key = {}
#        for k, tasks in self.optimization_history.items():
#            k = self._serialize_key(k)
#            minpos = self.minimum_positions[k]
#            final_opt_id = tasks[minpos]
#            map_id_key[final_opt_id] = k
#
#        return map_id_key
#
