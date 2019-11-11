import io
import pathlib
from typing import Any, Dict, Union

import numpy as np
import pandas as pd
from qcelemental.util.serialization import serialize

from ..interface.collections import HDF5View


class ViewHandler:
    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        """
        Parameters
        ----------
        path: Union[str, Path]
            Directory containing dataset views
        """
        self._view_cache: Dict[int, HDF5View] = {}
        self._path = pathlib.Path(path)
        if not self._path.is_dir():
            raise ValueError(f"Path in ViewHandler must be a directory, got: {self._path}")

    def view_path(self, collection_id: int) -> pathlib.Path:
        """
        Returns the path to a view corresponding to a collection identified by an id.

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
        Checks if view corresponding to a collection exists.

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

    def _get_view(self, collection_id: int):
        if collection_id not in self._view_cache:
            if not self.view_exists(collection_id):
                raise IOError
            self._view_cache[collection_id] = HDF5View(self.view_path(collection_id))
        return self._view_cache[collection_id]

    def handle_request(self, collection_id: int, request: str, model: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handles REST requests related to views. This function implements the GET endpoint
        /collections/[collection_id]/view/[request]

        Parameters
        ----------
        collection_id: int
            Collection id corresponding to a view.
        request: str
            Requested data. Allowed options and corresponding DatasetView methods:
            - list: list_values
            - value: get_values
            - molecule: get_molecules
            - entry: get_entries
        model:
            REST model containing input options.

        Returns
        -------
        Dict[str, Any]:
            Dictionary corresponding to requested REST model
        """
        meta = {"errors": [], "success": False, "error_description": False, "msgpacked_cols": []}

        try:
            view = self._get_view(collection_id)
        except IOError:
            meta["success"] = False
            meta["error_description"] = f"View not available for collection #{collection_id}"
            return {"meta": meta, "data": None}

        if request == "entry":
            try:
                df = view.get_entries(subset=model["subset"])
            except KeyError:
                meta["success"] = False
                meta["error_description"] = "Unable to find requested entry."
                return {"meta": meta, "data": None}
        elif request == "molecule":
            series = view.get_molecules(model["indexes"], keep_serialized=True)
            df = pd.DataFrame({"molecule": series})
            df.reset_index(inplace=True)
            meta["msgpacked_cols"].append("molecule")
        elif request == "value":
            df, units = view.get_values(model["queries"], subset=model["subset"])
            df.reset_index(inplace=True)
        elif request == "list":
            df = view.list_values()
        else:
            meta["success"] = False
            meta["error_description"] = f"Unknown view request: {request}."
            return {"meta": meta, "data": None}

        # msgpack columns not supported by pyarrow
        pack_columns = []
        for col in df.columns:
            if len(df) > 0:
                sample = df[col].iloc[0]
                if isinstance(sample, np.ndarray):
                    pack_columns.append(col)
                elif isinstance(sample, list):
                    pack_columns.append(col)
                # Add any other datatypes that need to be handled specially go here

        for col in pack_columns:
            df[col] = df[col].apply(lambda x: serialize(x, "msgpack-ext"))
        meta["msgpacked_cols"] += pack_columns

        # serialize
        f = io.BytesIO()
        df.to_feather(f)
        df_feather = f.getvalue()

        if request == "value":
            data = {"values": df_feather, "units": units}
        else:
            data = df_feather

        meta["success"] = True

        return {"meta": meta, "data": data}
