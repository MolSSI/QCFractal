import abc
import distutils
import hashlib
import pathlib
import shutil
import tarfile
import tempfile
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, NoReturn, Optional, Tuple, Union

import numpy as np
import pandas as pd
import h5py
from qcelemental.util.serialization import deserialize, serialize

from ..models import Molecule, ObjectId
from ..util import normalize_filename
from .dataset import Dataset, MoleculeEntry
from .reaction_dataset import ReactionDataset, ReactionEntry

if TYPE_CHECKING:  # pragma: no cover
    from .. import FractalClient
    from ..models.rest_models import CollectionSubresourceGETResponseMeta


class DatasetView(abc.ABC):
    @abc.abstractmethod
    def __init__(self) -> None:
        pass

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
    def get_values(
        self, queries: List[Dict[str, Union[str, bool]]], subset: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Get value columns.

        Parameters
        ----------
        queries: List[Dict[str, Union[str, bool]]]
            List of column metadata to match.
        subset: Optional[List[str]], optional
            The indices of the desired subset. Return all indices if subset is None.

        Returns
        -------
            A Dataframe whose columns correspond to each query and a dictionary of units for each column.
        """

    @abc.abstractmethod
    def get_molecules(self, indexes: List[Union[ObjectId, int]]) -> pd.Series:
        """
        Get a list of molecules using a molecule indexer

        Parameters
        ----------
        indexes : List['ObjectId']
            A list of molecule ids to return

        Returns
        -------
        pd.Series
            A Series of Molecules corresponding to indexes
        """

    @abc.abstractmethod
    def get_entries(self, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get a list of entries in the dataset

        Parameters
        ----------
        subset: Optional[List[str]], optional
            The indices of the desired subset. Return all indices if subset is None.

        Returns
        -------
        pd.DataFrame
            A dataframe of entries
        """


class HDF5View(DatasetView):
    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        """
        Parameters
        ----------
        path: Union[str, pathlib.Path]
            File path of view
        """
        path = pathlib.Path(path)
        self._path = path
        self._entries: pd.DataFrame = None
        self._index: pd.DataFrame = None

    def list_values(self) -> pd.DataFrame:
        with self._read_file() as f:
            history_keys = self._deserialize_field(f.attrs["history_keys"])
            df = pd.DataFrame(columns=history_keys + ["name", "native"])
            for dataset in f["value"].values():
                row = {k: self._deserialize_field(dataset.attrs[k]) for k in history_keys}
                row["name"] = self._deserialize_field(dataset.attrs["name"])
                row["native"] = True
                df = df.append(row, ignore_index=True)
            for dataset in f["contributed_value"].values():
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

    def get_values(
        self, queries: List[Dict[str, Union[str, bool]]], subset: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Parameters
        ----------
        subset
        queries: List[Dict[str, Union[str, bool]]]
            List of queries. Fields actually used are native, name, driver
        """

        units = {}
        entries = self.get_index(subset)
        indexes = entries._h5idx
        with self._read_file() as f:
            ret = pd.DataFrame(index=entries["index"])

            for query in queries:
                dataset_name = "value/" if query["native"] else "contributed_value/"
                dataset_name += self._normalize_hdf5_name(query["name"])
                driver = query["driver"]

                dataset = f[dataset_name]
                if not h5py.check_dtype(vlen=dataset.dtype):
                    data = [dataset[i] for i in indexes]
                else:
                    if driver.lower() == "gradient":
                        data = [np.reshape(dataset[i], (-1, 3)) for i in indexes]
                    elif driver.lower() == "hessian":
                        data = []
                        for i in indexes:
                            n2 = len(dataset[i])
                            n = int(round(np.sqrt(n2)))
                            data.append(np.reshape(dataset[i], (n, n)))
                    else:
                        warnings.warn(
                            f"Variable length data type not understood, returning flat array " f"(driver = {driver}).",
                            RuntimeWarning,
                        )
                        try:
                            data = [np.array(dataset[i]) for i in indexes]
                        except ValueError:
                            data = [dataset[i] for i in indexes]
                column_name = query["name"]
                column_units = self._deserialize_field(dataset.attrs["units"])
                ret[column_name] = data
                units[column_name] = column_units

        return ret, units

    def get_molecules(self, indexes: List[Union[ObjectId, int]], keep_serialized: bool = False) -> pd.Series:
        with self._read_file() as f:
            mol_schema = f["molecule/schema"]
            if not keep_serialized:
                mols = [
                    Molecule(
                        **self._deserialize_data(mol_schema[int(i) if isinstance(i, ObjectId) else i]), validate=False
                    )
                    for i in indexes
                ]
            else:
                mols = [mol_schema[int(i) if isinstance(i, ObjectId) else i].tobytes() for i in indexes]
        return pd.Series(mols, index=indexes)

    def get_index(self, subset: Optional[List[str]] = None) -> pd.DataFrame:
        if self._index is None:
            with self._read_file() as f:
                entry_group = f["entry"]
                self._index = pd.DataFrame({"index": entry_group["entry"][()]})
                self._index["index"] = self._index["index"].str.decode("utf-8")
                self._index["_h5idx"] = range(len(self._index))
                self._index.set_index("index", inplace=True)

        if subset is None:
            return self._index.reset_index()
        else:
            return self._index.loc[subset].reset_index()

    def get_entries(self, subset: Optional[List[str]] = None) -> pd.DataFrame:
        if self._entries is None:
            with self._read_file() as f:
                entry_group = f["entry"]
                if entry_group.attrs["model"] == "MoleculeEntry":
                    fields = ["name", "molecule_id"]
                elif entry_group.attrs["model"] == "ReactionEntry":
                    fields = ["name", "stoichiometry", "molecule", "coefficient"]
                else:
                    raise ValueError(
                        f"Unknown entry class ({entry_group.attrs['model']}) while " f"reading HDF5 entries."
                    )

                self._entries = pd.DataFrame({field: entry_group[field][()] for field in fields})

                # HDF5 stores these as byte arrays. But we use strings in pandas...
                self._entries["name"] = self._entries["name"].str.decode("utf-8")

                if entry_group.attrs["model"] == "ReactionEntry":
                    self._entries["stoichiometry"] = self._entries["stoichiometry"].str.decode("utf-8")

                self._entries.set_index("name", inplace=True)
        if subset is None:
            return self._entries.reset_index()
        else:
            return self._entries.loc[subset].reset_index()

    def write(self, ds: Dataset):
        # For data checksums
        dataset_kwargs = {"chunks": True, "fletcher32": True}
        ds.get_entries(force=True)
        n_records = len(ds.data.records)
        default_shape = (n_records,)

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
            "energy": {"dtype": np.dtype("float64"), "shape": default_shape},
            "gradient": {"dtype": vlen_double_t, "shape": default_shape},
            "hessian": {"dtype": vlen_double_t, "shape": default_shape},
            "dipole": {"dtype": np.dtype("float64"), "shape": (n_records, 3)},
        }

        def _write_dataset(dataset, column, entry_dset):
            assert column.shape[1] == 1
            for i, name in enumerate(entry_dset):
                name = name.decode("utf-8")
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
            for field in {
                "name",
                "collection",
                "provenance",
                "tagline",
                "tags",
                "id",
                "history_keys",
                "description",
                "metadata",
            }:
                f.attrs[field] = self._serialize_field(getattr(ds.data, field))
            if ds.client is not None:
                f.attrs["server_information"] = self._serialize_field(ds.client.server_information())
                f.attrs["server_address"] = self._serialize_field(ds.client.address)

            # Export molecules
            molecule_group = f.create_group("molecule")

            if "stoichiometry" in ds.data.history_keys:
                molecules = ds.get_molecules(stoich=list(ds.valid_stoich(force=True)), force=True)
            else:
                molecules = ds.get_molecules(force=True)
            mol_shape = (len(molecules),)
            mol_geometry = molecule_group.create_dataset(
                "geometry", shape=mol_shape, dtype=vlen_double_t, **dataset_kwargs
            )
            mol_symbols = molecule_group.create_dataset("symbols", shape=mol_shape, dtype=vlen_utf8_t, **dataset_kwargs)
            mol_schema = molecule_group.create_dataset("schema", shape=mol_shape, dtype=bytes_t, **dataset_kwargs)
            mol_charge = molecule_group.create_dataset(
                "charge", shape=mol_shape, dtype=np.dtype("float64"), **dataset_kwargs
            )
            mol_spin = molecule_group.create_dataset(
                "multiplicity", shape=mol_shape, dtype=np.dtype("int32"), **dataset_kwargs
            )
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
            entry_dset[:] = ds.get_index(force=True)

            entries = ds.get_entries(force=True)
            if isinstance(ds.data.records[0], MoleculeEntry):
                entry_group.attrs["model"] = "MoleculeEntry"
                entries["hdf5_molecule_id"] = entries["molecule_id"].map(mol_id_server_view)
                entry_group.create_dataset("name", data=entries["name"], dtype=utf8_t, **dataset_kwargs)
                entry_group.create_dataset(
                    "molecule_id", data=entries["hdf5_molecule_id"], dtype=np.dtype("int64"), **dataset_kwargs
                )
            elif isinstance(ds.data.records[0], ReactionEntry):
                entry_group.attrs["model"] = "ReactionEntry"
                entries["hdf5_molecule_id"] = entries["molecule"].map(mol_id_server_view)
                entry_group.create_dataset("name", data=entries["name"], dtype=utf8_t, **dataset_kwargs)
                entry_group.create_dataset(
                    "stoichiometry", data=entries["stoichiometry"], dtype=utf8_t, **dataset_kwargs
                )
                entry_group.create_dataset(
                    "molecule", data=entries["hdf5_molecule_id"], dtype=np.dtype("int64"), **dataset_kwargs
                )
                entry_group.create_dataset(
                    "coefficient", data=entries["coefficient"], dtype=np.dtype("float64"), **dataset_kwargs
                )
            else:
                raise ValueError(f"Unknown entry class ({type(ds.data.records[0])}) while writing HDF5 entries.")

            # Export native data columns
            value_group = f.create_group("value")
            history = ds.list_values(native=True, force=True).reset_index().to_dict("records")
            for specification in history:
                gv_spec = specification.copy()
                name = gv_spec.pop("name")
                if "stoichiometry" in gv_spec:
                    gv_spec["stoich"] = gv_spec.pop("stoichiometry")
                dataset_name = self._normalize_hdf5_name(name)
                df = ds.get_values(name=name, force=True, native=True)
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
                    if isinstance(cv_df[cv_name][0], float):
                        dataspec = {"dtype": np.dtype("float64"), "shape": default_shape}
                    elif isinstance(cv_df[cv_name][0], np.ndarray):
                        dataspec = {"dtype": vlen_double_t, "shape": default_shape}
                    else:
                        raise ValueError(
                            f"Unable to guess data specification for contributed value column named {cv_name}."
                        )
                    warnings.warn(
                        f"Contributed values column {cv_name} does not provide driver in theory_level_details. "
                        f"Inferred {dataspec}."
                    )

                dataset = contributed_group.create_dataset(
                    self._normalize_hdf5_name(cv_name), **dataspec, **dataset_kwargs
                )
                for field in [
                    "name",
                    "values_structure",
                    "theory_level",
                    "units",
                    "doi",
                    "external_url",
                    "citations",
                    "comments",
                    "theory_level",
                    "theory_level_details",
                ]:
                    dataset.attrs[field] = self._serialize_field(getattr(cv_model, field))

                _write_dataset(dataset, cv_df, entry_dset)

        # Clean up any caches
        self._entries = None

    def hash(self) -> str:
        """Returns the Blake2b hash of the view"""
        b2b = hashlib.blake2b()
        with open(self._path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                b2b.update(chunk)
        return b2b.hexdigest()

    @staticmethod
    def _normalize_hdf5_name(name: str) -> str:
        """Handles names with / in them, which is disallowed in HDF5"""
        if ":" in name:
            raise ValueError("':' not allowed in names")
        return name.replace("/", ":")

    @contextmanager
    def _read_file(self) -> Iterator["h5py.File"]:
        yield h5py.File(self._path, "r")

    @contextmanager
    def _write_file(self) -> Iterator["h5py.File"]:
        yield h5py.File(self._path, "w")

    # Methods for serializing to strings for storage in HDF5 metadata fields ("attrs")
    @staticmethod
    def _serialize_field(field: Any) -> str:
        return serialize(field, "json")

    @staticmethod
    def _deserialize_field(field: str) -> Any:
        return deserialize(field, "json")

    # Methods for serializing into HDF5 data fields
    @staticmethod
    def _serialize_data(data: Any) -> np.ndarray:
        # h5py v3 will support bytes,
        # but for now the workaround is variable-length np unit8
        return np.frombuffer(serialize(data, "msgpack-ext"), dtype="uint8")

    @staticmethod
    def _deserialize_data(data: np.ndarray) -> Any:
        return deserialize(data.tobytes(), "msgpack-ext")


class RemoteView(DatasetView):
    def __init__(self, client: "FractalClient", collection_id: int) -> None:
        """

        Parameters
        ----------
        client: FractalClient
        collection_id: int
        """
        self._client: FractalClient = client
        self._id: int = collection_id

    def get_entries(self, subset: Optional[List[str]] = None) -> pd.DataFrame:
        # TODO: consider adding a cache
        payload = {"meta": {}, "data": {"subset": subset}}

        response = self._client._automodel_request(f"collection/{self._id}/entry", "get", payload, full_return=True)
        self._check_response_meta(response.meta)
        return self._deserialize(response.data, response.meta.msgpacked_cols)

    def get_molecules(self, indexes: List[Union[ObjectId, int]]) -> pd.Series:
        payload = {"meta": {}, "data": {"indexes": indexes}}
        response = self._client._automodel_request(f"collection/{self._id}/molecule", "get", payload, full_return=True)
        self._check_response_meta(response.meta)
        df = self._deserialize(response.data, response.meta.msgpacked_cols)
        return df["molecule"].apply(lambda blob: Molecule(**blob, validate=False))

    def get_values(
        self, queries: List[Dict[str, Union[str, bool]]], subset: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Parameters
        ----------
        subset
        queries: List[Dict[str, Union[str, bool]]]
            List of queries. Fields actually used are native, name, driver
        """
        qlist = [{"name": query["name"], "driver": query["driver"], "native": query["native"]} for query in queries]
        payload = {"meta": {}, "data": {"queries": qlist, "subset": subset}}

        response = self._client._automodel_request(f"collection/{self._id}/value", "get", payload, full_return=True)
        self._check_response_meta(response.meta)
        return self._deserialize(response.data.values, response.meta.msgpacked_cols), response.data.units

    def list_values(self) -> pd.DataFrame:
        payload: Dict[str, Dict[str, Any]] = {"meta": {}, "data": {}}
        response = self._client._automodel_request(f"collection/{self._id}/list", "get", payload, full_return=True)
        self._check_response_meta(response.meta)
        return self._deserialize(response.data, response.meta.msgpacked_cols)

    def write(self, ds: Dataset) -> NoReturn:
        raise NotImplementedError()

    @staticmethod
    def _check_response_meta(meta: "CollectionSubresourceGETResponseMeta"):
        if not meta.success:
            raise RuntimeError(f"Remote view query failed with error message: {meta.error_description}")

    @staticmethod
    def _deserialize(data: bytes, msgpacked_cols: List[str]) -> pd.DataFrame:
        """
        Data are returned as feather-packed pandas DataFrames.
        Due to limitations in pyarrow, some objects are msgpacked inside the DataFrame.
        """
        import pyarrow

        df = pd.read_feather(pyarrow.BufferReader(data))
        for col in msgpacked_cols:
            df[col] = df[col].apply(lambda element: deserialize(element, "msgpack-ext"))

        if "index" in df.columns:
            df.set_index("index", inplace=True)  # pandas.to_feather does not support indexes,
            # so we have to send indexless frames over the wire, and set the index here.
        return df


class PlainTextView(DatasetView):
    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        """
        Parameters
        ----------
        path: Union[str, pathlib.Path]
            File path of view
        """
        path = pathlib.Path(path)
        if len(path.suffixes) == 0:
            path = path.with_suffix(".tar.gz")
        self._path = path

    def write(self, ds: Dataset) -> None:

        with tempfile.TemporaryDirectory() as tempd:
            temppath = pathlib.Path(tempd)
            mol_path = temppath / "molecules"
            entry_path = temppath / "entries.csv"
            value_path = temppath / "values.csv"
            list_path = temppath / "value_descriptions.csv"
            readme_path = temppath / "README"

            entries = ds.get_entries(force=True)
            # calculate molecule file name
            if isinstance(ds, ReactionDataset):
                entries["molecule filename"] = [
                    normalize_filename(f"{row[1]}__{row[2]}__{row[3]}") + ".xyz" for row in entries.itertuples()
                ]
                entries.rename(columns={"molecule": "molecule_id"}, inplace=True)
            elif isinstance(ds, Dataset):
                entries["molecule filename"] = [
                    normalize_filename(f"{row[1]}__{row[2]}") + ".xyz" for row in entries.itertuples()
                ]
            else:
                raise NotImplementedError(f"Unknown dataset type: {type(ds)}.")
            entries.to_csv(entry_path)

            mol_path.mkdir()
            molecules = ds._get_molecules(
                {row[1]: row[2] for row in entries[["molecule filename", "molecule_id"]].itertuples()}, force=True
            )
            for pathname, molecule in molecules.itertuples():
                molecule.to_file(mol_path / pathname)

            ds_query_limit_state = ds._disable_query_limit
            ds._disable_query_limit = True
            ds.get_values(force=True).to_csv(value_path)
            ds._disable_query_limit = ds_query_limit_state
            df = ds.list_values(force=True).reset_index().set_index("name")
            df["units"] = ds.units
            for name in df.index:
                if name in ds._column_metadata:
                    if "units" in ds._column_metadata[name]:
                        df["units"] = ds._column_metadata[name]["units"]
            df.to_csv(list_path)

            with open(readme_path, "w") as readme_file:
                readme_file.write(self._readme(ds))

            tarpath = temppath / "archive.tar.gz"
            with tarfile.open(tarpath, "w:gz") as tarball:
                for path in [mol_path, entry_path, value_path, list_path, readme_path]:
                    tarball.add(path, arcname=path.relative_to(temppath))

            shutil.move(tarpath, self._path)

    def list_values(self) -> NoReturn:
        raise NotImplementedError()

    def get_values(self, queries: List[Dict[str, Union[str, bool]]]) -> NoReturn:
        raise NotImplementedError()

    def get_molecules(self, indexes: List[Union[ObjectId, int]]) -> NoReturn:
        raise NotImplementedError()

    def get_entries(self) -> NoReturn:
        raise NotImplementedError()

    @staticmethod
    def _readme(ds) -> str:
        # TODO: citations once we add that column
        ret = f"""Name: {ds.name}

Tagline: {ds.data.tagline}
Tags: {", ".join(ds.data.tags)}
Downloaded from: {ds.client.server_information()['name']}

Description:
{ds.data.description}

Files included:
- values.csv: Table of computed values. Rows correspond to entries (e.g. molecules, reactions). Columns correspond to methods.
- value_descriptions.csv: Table of descriptions of columns in values.csv
- entries.csv: Table of descriptions of rows in values.csv
- molecules: Folder containing XYZ-formatted geometries of molecules in the dataset. Files are named by the molecule id found in entries.csv
"""

        return ret
