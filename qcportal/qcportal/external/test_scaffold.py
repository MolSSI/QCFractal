from __future__ import annotations

import os
import json
from typing import TYPE_CHECKING, Optional

import pytest

from qcfractal.components.singlepoint.testing_helpers import load_test_data, run_test_data
from qcportal.molecules import Molecule
from qcportal.singlepoint import SinglepointDataset
from qcportal.external.scaffold import to_json, from_json

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_dataset_client_query_dataset_records(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    run_test_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)

    filename = "test_dataset.json"
    to_json(filename, ds, compress=True)
    assert os.path.exists(filename+".bz2")
    os.remove(filename+".bz2")

    filename = "test_dataset.json"
    to_json(filename, ds)
    assert os.path.exists(filename)
    
    tmp_dict = json.load(open(filename, "r"))
    tmp_dict["metadata"]["name"] += "_test"
    json.dump(tmp_dict, open(filename, "w"))
    ds = from_json(filename, snowflake_client)
    os.remove(filename)

    # Query records belonging to a dataset
    rec_id_2 = ds.get_record("test_molecule_2", "spec_1").id
    mol_id_2 = ds.get_entry("test_molecule_2").molecule.id

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id)
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id, molecule_id=mol_id_2)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
    assert query_res_l[0].id == rec_id_2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id + 1, molecule_id=mol_id_2)
    assert query_res._fetched == 0

    # Query which dataset contains a record
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 1
    assert rec_info[0]["dataset_id"] == 1
    assert rec_info[0]["entry_name"] == "test_molecule_2"

    # Query which dataset contains a record
    ds.remove_records(entry_names="test_molecule_2", specification_names="spec_1", delete_records=True)
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 0