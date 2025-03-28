from __future__ import annotations

import os
import json
from typing import TYPE_CHECKING, Optional

import pytest

from deepdiff import DeepDiff

from qcfractal.components.singlepoint import testing_helpers as sp_testing_helpers
from qcfractal.components.optimization import testing_helpers as opt_testing_helpers
from qcfractal.components.torsiondrive import testing_helpers as td_testing_helpers
from qcfractal.components.gridoptimization import testing_helpers as go_testing_helpers
from qcfractal.components.manybody import testing_helpers as mb_testing_helpers
from qcfractal.components.reaction import testing_helpers as rxn_testing_helpers
from qcfractal.components.neb import testing_helpers as neb_testing_helpers
from qcportal.molecules import Molecule
from qcportal.singlepoint import SinglepointDataset
from qcportal.external.scaffold import to_json, from_json

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake

testing_helper = {
    "singlepoint": sp_testing_helpers,
    "optimization": opt_testing_helpers,
    "torsiondrive": td_testing_helpers,
    "gridoptimization": go_testing_helpers,
    "manybody": mb_testing_helpers,
    "reaction": rxn_testing_helpers,
    "neb": neb_testing_helpers,
}


@pytest.mark.parametrize(
    "dataset_type", ["singlepoint", "optimization", "torsiondrive", "gridoptimization", "manybody", "reaction", "neb"]
)
def test_dataset_client_query_dataset_records(snowflake: QCATestingSnowflake, dataset_type: str):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])
    if dataset_type == "singlepoint":
        ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test SP Dataset")
        assert ds.status() == {}

        input_spec, molecule, _ = testing_helper["singlepoint"].load_test_data("sp_psi4_peroxide_energy_wfn")
        testing_helper["singlepoint"].run_test_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

        ds.add_entry(name="test_molecule", molecule=molecule)
        ds.add_entry(name="test_molecule_2", molecule=molecule_2)
    elif dataset_type == "optimization":
        ds = snowflake_client.add_dataset("optimization", "Test Opt Dataset")
        assert ds.status() == {}

        input_spec, molecule, _ = testing_helper["optimization"].load_test_data("opt_psi4_benzene")
        testing_helper["optimization"].run_test_data(storage_socket, manager_name, "opt_psi4_benzene")

        ds.add_entry(name="test_molecule", initial_molecule=molecule)
        ds.add_entry(name="test_molecule_2", initial_molecule=molecule_2)
    elif dataset_type == "torsiondrive":
        ds = snowflake_client.add_dataset("torsiondrive", "Test TD Dataset")
        assert ds.status() == {}

        input_spec, molecules, _ = testing_helper["torsiondrive"].load_test_data("td_H2O2_psi4_pbe")
        testing_helper["torsiondrive"].run_test_data(storage_socket, manager_name, "td_H2O2_psi4_pbe")

        ds.add_entry(name="test_molecule", initial_molecules=molecules)
        ds.add_entry(name="test_molecule_2", initial_molecules=[molecule_2])
    elif dataset_type == "gridoptimization":
        ds = snowflake_client.add_dataset("gridoptimization", "Test GridOpt Dataset")
        assert ds.status() == {}

        input_spec, molecule, _ = testing_helper["gridoptimization"].load_test_data("go_H3NS_psi4_pbe")
        testing_helper["gridoptimization"].run_test_data(storage_socket, manager_name, "go_H3NS_psi4_pbe")

        ds.add_entry(name="test_molecule", initial_molecule=molecule)
        ds.add_entry(name="test_molecule_2", initial_molecule=molecule_2)
    elif dataset_type == "manybody":
        ds = snowflake_client.add_dataset("manybody", "Test Manybody Dataset")
        assert ds.status() == {}

        input_spec, molecule, _ = testing_helper["manybody"].load_test_data("mb_all_he4_psi4_multi")
        testing_helper["manybody"].run_test_data(storage_socket, manager_name, "mb_all_he4_psi4_multi")

        ds.add_entry(name="test_molecule", initial_molecule=molecule)
        ds.add_entry(name="test_molecule_2", initial_molecule=molecule_2)
    elif dataset_type == "reaction":
        ds = snowflake_client.add_dataset("reaction", "Test Reaction Dataset")
        assert ds.status() == {}

        input_spec, stoichiometries, _ = testing_helper["reaction"].load_test_data("rxn_H2O_psi4_b3lyp_sp")
        testing_helper["reaction"].run_test_data(storage_socket, manager_name, "rxn_H2O_psi4_b3lyp_sp")

        ds.add_entry(name="test_molecule", stoichiometries=stoichiometries)
    elif dataset_type == "neb":
        ds = snowflake_client.add_dataset("neb", "Test NEB Dataset")
        assert ds.status() == {}

        input_spec, molecules, _ = testing_helper["neb"].load_test_data("neb_HCN_psi4_pbe")
        testing_helper["neb"].run_test_data(storage_socket, manager_name, "neb_HCN_psi4_pbe")

        ds.add_entry(name="test_molecule", initial_chain=molecules)
        ds.add_entry(name="test_molecule_2", initial_chain=[molecule_2])

    ds.add_specification("spec_1", input_spec)

    filename = "test_dataset.json"
    to_json(ds, filename, compress=True)
    assert os.path.exists(filename + ".bz2")
    os.remove(filename + ".bz2")

    to_json(ds, filename)
    assert os.path.exists(filename)

    tmp_dict = json.load(open(filename, "r"))
    tmp_dict["metadata"]["name"] += "_test"
    json.dump(tmp_dict, open(filename, "w"))
    ds2 = from_json(filename, snowflake_client)
    os.remove(filename)

    diff = DeepDiff(ds, ds2)
    assert len(diff) == 1 and len(diff["values_changed"]) == 2  # Dataset ids and names change
