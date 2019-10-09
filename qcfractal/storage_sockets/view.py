import io
import pathlib
from typing import Any, Dict, Union

import numpy as np
import pandas as pd

from qcelemental.util.serialization import serialize

# TODO(mattwelborn): names are hard
from ..interface.collections import HDF5View
from .storage_utils import get_metadata_template


class ViewHandler:
    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        """
        Parameters
        ----------
        path: Union[str, Path]
            Directory containing dataset views
        """
        self._path = pathlib.Path(path)
        if not self._path.is_dir():
            raise ValueError(f"Path in ViewHandler must be a directory, got: {self._path}")

    def view_path(self, collection_id: int) -> pathlib.Path:
        """
        Get path of a view

        Parameters
        ----------
        collection_id: int
            Collection id corresponding to view

        Returns
        -------
        pathlib.Path
            Path of requested view
        """

        return self._path / f"{collection_id}.hdf5"

    def view_exists(self, collection_id: int) -> bool:
        """
        Check if view corresponding to a collection exists

        Parameters
        ----------
        collection_id: int
            Collection id corresponding to view

        Returns
        -------
        bool
            Does the view exist?
        """

        return self.view_path(collection_id).is_file()

    def handle_request(self, collection_id: int, request: str, model: Dict[str, Any]) -> Dict[str, Any]:
        """

        Parameters
        ----------
        collection_id
        request
        model

        Returns
        -------

        """
        meta = {"errors": [], "success": False, "error_description": False, "msgpacked_cols": []}

        if not self.view_exists(collection_id):
            meta["success"] = False
            meta["error_description"] = f"View not available for collection #{collection_id}"
            return {"meta": meta, "data": None}

        view = HDF5View(self.view_path(collection_id))
        if request == "entry":
            df = view.get_entries()
        elif request == "molecule":
            series = view.get_molecules(model["indexes"], keep_serialized=True)
            df = pd.DataFrame({'molecule': series})
        elif request == "values":
            df, units = view.get_values(model["queries"])
        elif request == "list":
            df = view.list_values()
        else:
            meta["success"] = False
            meta["error_description"] = f"Unknown view request {request}."
            return {"meta": meta, "data": None}

        # msgpack columns not supported by pyarrow
        msgpack_cols = []
        for col in df.columns:
            if len(df) > 0:
                sample = df.loc[0, col]
                if isinstance(sample, np.ndarray):
                    if len(sample.shape) > 1:
                        msgpack_cols.append(col)
                # Add any other datatypes that need to be handled specially here

        for col in msgpack_cols:
            df[col] = df[col].apply(lambda x: serialize(x, 'msgpack-ext'))

        # serialize
        f = io.BytesIO()
        df.to_feather(f)
        df_feather = f.getvalue()

        if request == "values":
            data = (df_feather, units)
        else:
            data = df_feather

        meta["success"] = True
        meta["msgpacked_cols"] = msgpack_cols

        return {"meta": meta, "data": data}
