"""
QCPortal Database ODM
"""
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

import pandas as pd
import qcelemental as qcel

from ..models import ObjectId, OptimizationSpecification, ProtoModel, QCSpecification
from .collection import BaseProcedureDataset
from .collection_utils import register_collection

if TYPE_CHECKING:  # pragma: no cover
    from ..models import Molecule


class OptEntry(ProtoModel):
    """Data model for the optimizations in a Dataset"""

    name: str
    initial_molecule: ObjectId
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    object_map: Dict[str, ObjectId] = {}


class OptEntrySpecification(ProtoModel):
    name: str
    description: Optional[str]
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification
    protocols: qcel.models.procedures.OptimizationProtocols = qcel.models.procedures.OptimizationProtocols()


class OptimizationDataset(BaseProcedureDataset):
    class DataModel(BaseProcedureDataset.DataModel):

        records: Dict[str, OptEntry] = {}
        history: Set[str] = set()
        specs: Dict[str, OptEntrySpecification] = {}

        class Config(BaseProcedureDataset.DataModel.Config):
            pass

    def _internal_compute_add(self, spec: Any, entry: Any, tag: str, priority: str) -> ObjectId:

        # Form per-procedure keywords dictionary
        general_keywords = spec.optimization_spec.keywords
        if general_keywords is None:
            general_keywords = {}
        keywords = {**general_keywords, **entry.additional_keywords}

        procedure_parameters = {
            "keywords": keywords,
            "qc_spec": spec.qc_spec.dict(),
            "protocols": spec.protocols.dict(),
        }

        return self.client.add_procedure(
            "optimization",
            spec.optimization_spec.program,
            procedure_parameters,
            [entry.initial_molecule],
            tag=tag,
            priority=priority,
        ).ids[0]

    def add_specification(
        self,
        name: str,
        optimization_spec: OptimizationSpecification,
        qc_spec: QCSpecification,
        description: Optional[str] = None,
        protocols: Optional[Dict[str, Any]] = None,
        overwrite=False,
    ) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the specification
        optimization_spec : OptimizationSpecification
            A full optimization specification for Optimization
        qc_spec : QCSpecification
            A full quantum chemistry specification for Optimization
        description : str, optional
            A short text description of the specification
        protocols : Optional[Dict[str, Any]], optional
            Protocols for this specification.
        overwrite : bool, optional
            Overwrite existing specification names
        """
        if protocols is None:
            protocols = {}

        spec = OptEntrySpecification(
            name=name,
            optimization_spec=optimization_spec,
            qc_spec=qc_spec,
            description=description,
            protocols=protocols,
        )

        return self._add_specification(name, spec, overwrite=overwrite)

    def add_entry(
        self,
        name: str,
        initial_molecule: "Molecule",
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        save: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the entry, will be used for the index
        initial_molecule : Molecule
            The list of starting Molecules for the Optimization
        additional_keywords : Dict[str, Any], optional
            Additional keywords to add to the optimization run
        attributes : Dict[str, Any], optional
            Additional attributes and descriptions for the entry
        save : bool, optional
            If true, saves the collection after adding the entry. If this is False be careful
            to call save after all entries are added, otherwise data pointers may be lost.
        """

        self._check_entry_exists(name)  # Fast skip

        if additional_keywords is None:
            additional_keywords = {}

        if attributes is None:
            attributes = {}

        # Build new objects
        molecule_id = self.client.add_molecules([initial_molecule])[0]
        entry = OptEntry(
            name=name, initial_molecule=molecule_id, additional_keywords=additional_keywords, attributes=attributes
        )

        self._add_entry(name, entry, save)

    def counts(
        self, entries: Optional[Union[str, List[str]]] = None, specs: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Counts the number of optimization or gradient evaluations associated with the
        Optimizations.

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

        def count_gradients(opt):
            if (not hasattr(opt, "status")) or opt.status != "COMPLETE":
                return None
            return len(opt.energies)

        # Loop over the data and apply the count function
        ret = []
        for col in specs:
            data = self.df[col]
            if entries:
                data = data[entries]

            cnts = data.apply(lambda td: count_gradients(td))
            ret.append(cnts)

        ret = pd.DataFrame(ret).transpose()
        ret.dropna(inplace=True, how="all")
        # ret = pd.DataFrame([ret[x].astype(int) for x in ret.columns]).transpose()
        return ret


register_collection(OptimizationDataset)
