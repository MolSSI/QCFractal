import abc
import warnings
from contextlib import contextmanager

from .dataset import Dataset
import pathlib
from typing import Union, List, Tuple
import numpy as np
import pandas as pd
import h5py

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
        pass

    @abc.abstractmethod
    def list_values(self) -> pd.Dataframe:
        """
        Get a list of all available value columns.

        Returns
        -------
            A Dataframe with specification of available columns.
        """
        pass

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
        pass


class HDF5View(DatasetView):

    def __init__(self, path: Union[str, pathlib.Path]):
        super.__init__(path)

    @contextmanager
    def read_file(self):
        yield h5py.File(self.path, 'r')

    @contextmanager
    def write_file(self):
        yield h5py.File(self.path, 'w')

    def list_values(self):


    def get_values(self, queries: List[Tuple[str]]) -> Tuple[pd.DataFrame, List[str]]:
        units = []
        with self.read_file() as f:
            ret = pd.DataFrame(index=f["entry"][:])

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
                        warnings.warn(f"Variable length data type not understood, returning flat array "
                                      f"(driver = {driver}).", RuntimeWarning)
                        data = list(dataset[:])
                column_name = dataset.attrs["name"]
                column_units = dataset.attrs["units"]
                ret[column_name] = data
                units.append(column_units)

        return ret, units

    @staticmethod
    def _normalize_hdf5_name(name: str) -> str:
        """ Handles names with / in them, which is disallowed in HDF5 """
        if ":" in name:
            raise ValueError("':' not allowed in names")
        return name.replace("/", ":")