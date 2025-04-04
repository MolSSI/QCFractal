"""Functions to export and import elect information from QCFractal datasets

This module will allow users to write and read a json file that can be reformed into an initial dataset
**before** submission. This means that records are not present.

| Organization: Open Molecular Software Foundation: Open Force Field
| Author: Jennifer A Clark
| Date: March 13 2025
"""

import json
import bz2

from ..serialization import encode_to_json


def to_json(ds, filename="scaffold.json", indent=4, compress=False):
    """Export a QCFractal dataset to a json file.

    Can be imported with :func:`fom_json` to make a new dataset.

    Args:
        ds (qcportal.*Dataset): QCFractal dataset
        filename (str, optional): Filename/path to store output json file. Defaults to "scaffold.json".
        indent (int, optional): Level of indent for the output json file. Defaults to 4.
        compress (bool, optional): If True, will compress to bz2. Defaults to False.
    """

    inputs = [
        "dataset_type",
        "name",
        "description",
        "tagline",
        "tags",
        "group",
        "provenance",
        "visibility",
        "default_tag",
        "default_priority",
        "metadata",
        "owner_group",
    ]  # Inputs for client.add_dataset(
    metadata = {key: value for key, value in ds.dict().items() if key in inputs}
    d = {
        "metadata": metadata,
        "entries": {entry.name: entry for entry in ds.iterate_entries()},
        "specifications": ds.specifications,
    }
    d_serializable = encode_to_json(d)

    if compress:
        with bz2.open(filename + ".bz2", "wt", encoding="utf-8") as f:
            json.dump(d_serializable, f, ensure_ascii=False, indent=indent)
    else:
        with open(filename, "w") as f:
            json.dump(d_serializable, f, indent=indent)


def from_json(filename, client):
    """Create a QCFractal dataset from a json file.

    Created from output of :func:`to_json`. This allows a user to save the "state"
    of a dataset before submission.

    Args:
        filename (str): Filename/path to imported json file.
        client (qcportal.client.PortalClient): Client to which the dataset will be added.

    Returns:
        qcportal.*Dataset: QCFractal dataset. This dataset is not submitted in this function.
    """

    extension = filename.split(".")[-2:]
    if extension[-1] == "json":
        with open(filename, "r") as f:
            ds_dict = json.load(f)
    elif extension[-1] == "bz2" and extension[-2] == "json":
        with bz2.open(filename, "rt", encoding="utf-8") as f:
            ds_dict = json.load(f)
    else:
        raise ValueError(f"File extension must be json or json.bz2, not {extension[-2]}.{extension[-1]}")

    ds = client.add_dataset(**ds_dict["metadata"])

    for _, spec in ds_dict["specifications"].items():
        ds.add_specification(**spec)

    entries = []
    entry_type = ds._entry_type
    for _, entry in ds_dict["entries"].items():
        if "local_results" in entry:
            del entry["local_results"]
        entries.append(entry_type(**entry))
    ds.background_add_entries(entries)

    return ds
