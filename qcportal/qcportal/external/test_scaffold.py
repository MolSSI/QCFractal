from __future__ import annotations

import os
import json
from typing import TYPE_CHECKING, Optional

import pytest

from qcfractal.components import singlepoint, optimization, torsiondrive, gridoptimization, manybody, reaction, neb
from qcportal.molecules import Molecule
from qcportal.singlepoint import SinglepointDataset
from qcportal.external.scaffold import to_json, from_json

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake

@pytest.mark.parametrize(
    "dataset_type", ["singlepoint", "optimization", "torsiondrive", "gridoptimization", "manybody", "reaction", "neb"]
)
def test_dataset_client_query_dataset_records(snowflake: QCATestingSnowflake, dataset_type: str):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    if dataset_type == "singlepoint":
        ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
        assert ds.status() == {}
    
        input_spec, molecule, _ = singlepoint.testing_helpers.load_test_data("sp_psi4_peroxide_energy_wfn")
        singlepoint.testing_helpers.run_test_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")
    elif dataset_type == "optimization":
        ds = snowflake_client.add_dataset("optimization", "Test dataset")
        assert ds.status() == {}
        
        input_spec, molecule, _ = optimization.testing_helpers.load_test_data("opt_psi4_benzene")
        optimization.testing_helpers.run_test_data(storage_socket, manager_name, "opt_psi4_benzene")
    elif dataset_type == "torsiondrive":
        ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
        assert ds.status() == {}
        
        input_spec, molecule, _ = torsiondrive.testing_helpers.load_test_data("td_H2O2_psi4_pbe")
        torsiondrive.testing_helpers.run_test_data(storage_socket, manager_name, "td_H2O2_psi4_pbe")
    elif dataset_type == "gridoptimization":
        ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
        assert ds.status() == {}
        
        input_spec, molecule, _ = gridoptimization.testing_helpers.load_test_data("go_H3NS_psi4_pbe")
        gridoptimization.testing_helpers.run_test_data(storage_socket, manager_name, "go_H3NS_psi4_pbe")
    elif dataset_type == "manybody":
        ds = snowflake_client.add_dataset("manybody", "Test dataset")
        assert ds.status() == {}
        
        input_spec, molecule, _ = manybody.testing_helpers.load_test_data("mb_all_he4_psi4_multi")
        manybody.testing_helpers.run_test_data(storage_socket, manager_name, "mb_all_he4_psi4_multi")
    elif dataset_type == "reaction":
        ds = snowflake_client.add_dataset("reaction", "Test dataset")
        assert ds.status() == {}
        
        input_spec, molecule, _ = reaction.testing_helpers.load_test_data("rxn_H2O_psi4_b3lyp_sp")
        reaction.testing_helpers.run_test_data(storage_socket, manager_name, "rxn_H2O_psi4_b3lyp_sp")
    elif dataset_type == "neb":
        ds = snowflake_client.add_dataset("neb", "Test dataset")
        assert ds.status() == {}
        
        input_spec, molecule, _ = neb.testing_helpers.load_test_data("neb_HCN_psi4_pbe")
        neb.testing_helpers.run_test_data(storage_socket, manager_name, "neb_HCN_psi4_pbe")
    
    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])
    
    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)

    filename = "test_dataset.json"
    to_json(filename, ds, compress=True)
    assert os.path.exists(filename+".bz2")
    os.remove(filename+".bz2")

    to_json(filename, ds)
    assert os.path.exists(filename)
    
    tmp_dict = json.load(open(filename, "r"))
    tmp_dict["metadata"]["name"] += "_test"
    json.dump(tmp_dict, open(filename, "w"))
    ds2 = from_json(filename, snowflake_client)
    os.remove(filename)
        
    # Check records
    if dataset_type == "singlepoint":
        opt_hashes = {rec.molecule.get_hash() for _, _, rec in ds.iterate_records()}
        opt_hashes_2 = {rec.molecule.get_hash() for _, _, rec in ds2.iterate_records()}
    elif dataset_type == "optimization":
        opt_hashes = {rec.initial_molecule().get_hash() for rec in ds.iterate_records()}
        opt_hashes_2 = {rec.initial_molecule().get_hash() for rec in ds2.iterate_records()}
    elif dataset_type == "torsiondrive":
        opt_hashes = {rec.initial_molecules[0].get_hash() for _, _, rec in ds.iterate_records()}
        opt_hashes_2 = {rec.initial_molecules[0].get_hash() for _, _, rec in ds2.iterate_records()}
    elif dataset_type == "gridoptimization":
        opt_hashes = {rec.initial_molecule().get_hash() for rec in ds.iterate_records()}
        opt_hashes_2 = {rec.initial_molecule().get_hash() for rec in ds2.iterate_records()}
    elif dataset_type == "manybody":
        opt_hashes = {rec.initial_molecule().get_hash() for rec in ds.iterate_records()}
        opt_hashes_2 = {rec.initial_molecule().get_hash() for rec in ds2.iterate_records()}
    elif dataset_type == "reaction":
        opt_hashes = {rec.components[0].singlepoint_record.molecule().get_hash() for rec in ds.iterate_records()}
        opt_hashes_2 = {rec.components[0].singlepoint_record.molecule().get_hash() for rec in ds2.iterate_records()}
    elif dataset_type == "neb":
        opt_hashes = {list(rec.singlepoints.values())[0][0].molecule().get_hash() for rec in ds.iterate_records()}
        opt_hashes_2 = {list(rec.singlepoints.values())[0][0].molecule().get_hash() for rec in ds2.iterate_records()}

    assert opt_hashes == opt_hashes_2
        