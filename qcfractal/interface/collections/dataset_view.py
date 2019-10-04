import abc
import distutils
from qcelemental.util.serialization import serialize, deserialize
import warnings
from contextlib import contextmanager

from .dataset import Dataset, MoleculeEntry
from .reaction_dataset import ReactionEntry
import pathlib
from typing import Union, List, Tuple, Any
import numpy as np
import pandas as pd
import h5py

from ..models import Molecule


class DatasetView(abc.ABC):
    def __init__(self, path: Union[str, pathlib.Path]):
        """
        Parameters
        ----------
        path: Union[str, pathlib.Path]
            File path of view
        """
        if isinstance(path, str):
            path = pathlib.Path(path)
        self._path = path

    @abc.abstractmethod
    def write(self, ds: Dataset) -> None:
        """
        Writes a dataset to disk.

        Parameters
        ----------
        ds: Dataset
            The dataset to write.

        Returns
        -------
            None
        """
    @abc.abstractmethod
    def list_values(self) -> pd.DataFrame:
        """
        Get a list of all available value columns.

        Returns
        -------
            A Dataframe with specification of available columns.
        """
    @abc.abstractmethod
    def get_values(self, queries: List[Tuple[str]]) -> Tuple[pd.DataFrame, List[str]]:
        """
        Get value columns.

        Parameters
        ----------
        queries: List[Tuple[str]]
            List of column metadata to match.

        Returns
        -------
            A Dataframe whose columns correspond to each query and a list of units for each column.
        """
    @abc.abstractmethod
    def get_molecules(self, indexes: List['ObjectId']) -> List[Molecule]:
        """
        Get a list of molecules using a molecule indexer

        Parameters
        ----------
        indexes : List['ObjectId']
            A list of molecule ids to return

        Returns
        -------
        List['Molecule']
            A list of Molecules corresponding to indexes
        """
    @abc.abstractmethod
    def get_entries(self) -> pd.DataFrame:
        """
        Get a list of entries in the dataset

        Returns
        -------
        pd.DataFrame
            A dataframe of entries
        """


class HDF5View(DatasetView):
    def __init__(self, path: Union[str, pathlib.Path]):
        super().__init__(path)
        self._entries: pd.DataFrame = None

    def list_values(self) -> pd.DataFrame:
        df = pd.DataFrame()
        with self._read_file() as f:
            history_keys = self._deserialize_field(f.attrs['history_keys'])
            for dataset in f['value'].values():
                row = {k: self._deserialize_field(dataset.attrs[k]) for k in history_keys}
                row["name"] = self._deserialize_field(dataset.attrs["name"])
                row["native"] = True
                df = df.append(row, ignore_index=True)
            for dataset in f['contributed_value'].values():
                row = dict()
                row["name"] = self._deserialize_field(dataset.attrs["name"])
                for k in history_keys:
                    row[k] = "Unknown"
                # ReactionDataset uses "default" as a default value for stoich, but many contributed datasets lack a stoich field
                if "stoichiometry" in history_keys:
                    row["stoichiometry"] = "default"
                if "theory_level_details" in dataset.attrs:
                    theory_level_details = self._deserialize_field(dataset.attrs["theory_level_details"])
                    if isinstance(theory_level_details, dict):
                        row.update(**theory_level_details)
                row["native"] = False
                df = df.append(row, ignore_index=True)
        # for some reason, pandas makes native a float column
        return df.astype({"native": bool})

    def get_values(self, queries: List[Tuple[str]]) -> Tuple[pd.DataFrame, List[str]]:
        units = {}
        with self._read_file() as f:
            ret = pd.DataFrame(index=f["entry/entry"][()])

            for query in queries:
                dataset_name = "value/" if query["native"] else "contributed_value/"
                dataset_name += self._normalize_hdf5_name(query["name"])
                driver = query["driver"]

                dataset = f[dataset_name]
                if not h5py.check_dtype(vlen=dataset.dtype):
                    data = list(dataset[:])
                else:
                    nentries = dataset.shape[0]
                    if driver.lower() == "gradient":
                        data = [np.reshape(dataset[i], (-1, 3)) for i in range(nentries)]
                    elif driver.lower() == "hessian":
                        data = []
                        for i in range(nentries):
                            n2 = len(dataset[i])
                            n = int(round(np.sqrt(n2)))
                            data.append(np.reshape(dataset[i], (n, n)))
                    else:
                        warnings.warn(
                            f"Variable length data type not understood, returning flat array "
                            f"(driver = {driver}).", RuntimeWarning)
                        data = list(dataset[:])
                column_name = query["name"]
                column_units = self._deserialize_field(dataset.attrs["units"])
                ret[column_name] = data
                units[column_name] = column_units

        return ret, units

    def get_molecules(self, indexes: List['ObjectId']) -> List[Molecule]:
        with self._read_file() as f:
            mol_schema = f['molecule/schema']
            ret = [Molecule(**self._deserialize_data(mol_schema[i])) for i in indexes]
        return ret

    def get_entries(self) -> pd.DataFrame:
        if self._entries is None:
            with self._read_file() as f:
                entry_group = f["entry"]
                if entry_group.attrs["model"] == "MoleculeEntry":
                    fields = ("name", "molecule_id")
                elif entry_group.attrs["model"] == "ReactionEntry":
                    fields = ("name", "stoichiometry", "molecule", "coefficient")
                else:
                    raise ValueError(f"Unknown entry class ({entry_group.attrs['model']}) while "
                                     f"reading HDF5 entries.")
                self._entries = pd.DataFrame({field: entry_group[field][()] for field in fields})
        return self._entries

    def write(self, ds: Dataset):
        # For data checksums
        dataset_kwargs = {"chunks": True, "fletcher32": True}

        n_records = len(ds.data.records)
        default_shape = (n_records, )

        if h5py.__version__ >= distutils.version.StrictVersion("2.10.0"):
            vlen_double_t = h5py.vlen_dtype(np.dtype("float64"))
            utf8_t = h5py.string_dtype(encoding="utf-8")
            bytes_t = h5py.vlen_dtype(np.dtype("uint8"))
            vlen_utf8_t = h5py.vlen_dtype(utf8_t)
        else:
            vlen_double_t = h5py.special_dtype(vlen=np.dtype("float64"))
            utf8_t = h5py.special_dtype(vlen=str)
            bytes_t = h5py.special_dtype(vlen=np.dtype("uint8"))
            vlen_utf8_t = h5py.special_dtype(vlen=utf8_t)

        driver_dataspec = {
            "energy": {
                "dtype": np.dtype("float64"),
                "shape": default_shape
            },
            "gradient": {
                "dtype": vlen_double_t,
                "shape": default_shape
            },
            "hessian": {
                "dtype": vlen_double_t,
                "shape": default_shape
            },
            "dipole": {
                "dtype": np.dtype("float64"),
                "shape": (n_records, 3)
            }
        }

        def _write_dataset(dataset, column, entry_dset):
            assert column.shape[1] == 1
            for i, name in enumerate(entry_dset):
                element = column.loc[name][0]
                if not h5py.check_dtype(vlen=dataset.dtype):
                    dataset[i] = element
                # Variable length datatypes require flattening of the array and special handling of missing values
                else:
                    try:
                        dataset[i] = element.ravel()
                    except AttributeError:
                        if np.isnan(element):
                            pass
                        else:
                            raise

        with self._write_file() as f:
            # Collection attributes
            for field in {"name", "collection", "provenance", "tagline", "tags", "id", "history_keys"}:
                f.attrs[field] = self._serialize_field(getattr(ds.data, field))
            f.attrs["server_information"] = self._serialize_field(ds.client.server_information())

            # Export molecules
            molecule_group = f.create_group("molecule")

            if "stoichiometry" in ds.data.history_keys:
                molecules = ds.get_molecules(stoich=list(ds.valid_stoich), force=True)
            else:
                molecules = ds.get_molecules(force=True)
            mol_shape = (len(molecules), )
            mol_geometry = molecule_group.create_dataset("geometry",
                                                         shape=mol_shape,
                                                         dtype=vlen_double_t,
                                                         **dataset_kwargs)
            mol_symbols = molecule_group.create_dataset("symbols",
                                                        shape=mol_shape,
                                                        dtype=vlen_utf8_t,
                                                        **dataset_kwargs)
            mol_schema = molecule_group.create_dataset("schema", shape=mol_shape, dtype=bytes_t, **dataset_kwargs)
            mol_charge = molecule_group.create_dataset("charge",
                                                       shape=mol_shape,
                                                       dtype=np.dtype('float64'),
                                                       **dataset_kwargs)
            mol_spin = molecule_group.create_dataset("multiplicity",
                                                     shape=mol_shape,
                                                     dtype=np.dtype('int32'),
                                                     **dataset_kwargs)
            mol_id_server_view = {}
            for i, mol_row in enumerate(molecules.to_dict("records")):
                molecule = mol_row["molecule"]
                mol_geometry[i] = molecule.geometry.ravel()
                mol_schema[i] = self._serialize_data(molecule)
                mol_symbols[i] = molecule.symbols
                mol_charge[i] = molecule.molecular_charge
                mol_spin[i] = molecule.molecular_multiplicity
                mol_id_server_view[molecule.id] = i

            # Export entries
            entry_group = f.create_group("entry")
            entry_dset = entry_group.create_dataset("entry", shape=default_shape, dtype=utf8_t, **dataset_kwargs)
            entry_dset[:] = ds.get_index()

            entries = ds.get_entries(force=True)
            if isinstance(ds.data.records[0], MoleculeEntry):
                entry_group.attrs["model"] = "MoleculeEntry"
                entries["hdf5_molecule_id"] = entries["molecule_id"].map(mol_id_server_view)
                entry_group.create_dataset("name", data=entries["name"], dtype=utf8_t, **dataset_kwargs)
                entry_group.create_dataset("molecule_id",
                                           data=entries["hdf5_molecule_id"],
                                           dtype=np.dtype("int64"),
                                           **dataset_kwargs)
            elif isinstance(ds.data.records[0], ReactionEntry):
                entry_group.attrs["model"] = "ReactionEntry"
                entries["hdf5_molecule_id"] = entries["molecule"].map(mol_id_server_view)
                entry_group.create_dataset("name", data=entries["name"], dtype=utf8_t, **dataset_kwargs)
                entry_group.create_dataset("stoichiometry",
                                           data=entries["stoichiometry"],
                                           dtype=utf8_t,
                                           **dataset_kwargs)
                entry_group.create_dataset("molecule",
                                           data=entries["hdf5_molecule_id"],
                                           dtype=np.dtype("int64"),
                                           **dataset_kwargs)
                entry_group.create_dataset("coefficient",
                                           data=entries["coefficient"],
                                           dtype=np.dtype("float64"),
                                           **dataset_kwargs)
            else:
                raise ValueError(f"Unknown entry class ({type(ds.data.records[0])}) while " f"writing HDF5 entries.")

            # Export native data columns
            value_group = f.create_group("value")
            history = ds.list_values(native=True, force=True).reset_index().to_dict("records")
            for specification in history:
                gv_spec = specification.copy()
                name = gv_spec.pop("name")
                if "stoichiometry" in gv_spec:
                    gv_spec["stoich"] = gv_spec.pop("stoichiometry")
                dataset_name = self._normalize_hdf5_name(name)
                df = ds.get_values(**gv_spec, force=True)
                assert df.shape[1] == 1

                driver = specification["driver"]
                dataspec = driver_dataspec[driver]
                dataset = value_group.create_dataset(dataset_name, **dataspec, **dataset_kwargs)

                for key in specification:
                    dataset.attrs[key] = self._serialize_field(specification[key])
                dataset.attrs["units"] = self._serialize_field(ds.units)

                _write_dataset(dataset, df, entry_dset)

            # Export contributed data columns
            contributed_group = f.create_group("contributed_value")
            for cv_name in ds.list_values(force=True, native=False)["name"]:
                cv_df = ds.get_values(name=cv_name, force=True, native=False)
                cv_model = ds.data.contributed_values[cv_name.lower()]

                try:
                    dataspec = driver_dataspec[cv_model.theory_level_details["driver"]]
                except (KeyError, TypeError):
                    warnings.warn(
                        f"Contributed values column {cv_name} does not provide driver in theory_level_details. "
                        f"Assuming default driver for the dataset ({ds.data.default_driver}).")
                    dataspec = driver_dataspec[ds.data.default_driver]

                dataset = contributed_group.create_dataset(self._normalize_hdf5_name(cv_name), **dataspec,
                                                           **dataset_kwargs)
                for field in {
                        "name", "theory_level", "units", "doi", "comments", "theory_level", "theory_level_details"
                }:
                    dataset.attrs[field] = self._serialize_field(getattr(cv_model, field))

                _write_dataset(dataset, cv_df, entry_dset)

        # Clean up any caches
        self._entries = None

    @staticmethod
    def _normalize_hdf5_name(name: str) -> str:
        """ Handles names with / in them, which is disallowed in HDF5 """
        if ":" in name:
            raise ValueError("':' not allowed in names")
        return name.replace("/", ":")

    @contextmanager
    def _read_file(self):
        yield h5py.File(self._path, 'r')

    @contextmanager
    def _write_file(self):
        yield h5py.File(self._path, 'w')

    # Methods for serializing to strings for storage in HDF5 metadata fields ("attrs")
    @staticmethod
    def _serialize_field(field: Any) -> str:
        return serialize(field, 'json')

    @staticmethod
    def _deserialize_field(field: str) -> Any:
        return deserialize(field, 'json')

    # Methods for serializing into HDF5 data fields
    @staticmethod
    def _serialize_data(data: Any) -> np.ndarray:
        return np.fromstring(serialize(data, 'msgpack-ext'), dtype='uint8')

    @staticmethod
    def _deserialize_data(data: np.ndarray) -> Any:
        return deserialize(data.tobytes(), 'msgpack-ext')
