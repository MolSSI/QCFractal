from __future__ import annotations

from typing import TYPE_CHECKING

from qcarchivetesting import load_molecule_data
from qcfractal.components.optimization.testing_helpers import (
    load_test_data as load_optimization_test_data,
    run_test_data as run_optimization_test_data,
)

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_singlepoint_dataset_model_entries_from_1(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    opt_ds = snowflake_client.add_dataset("optimization", "Test optimization dataset")

    # Slight cheat
    # Run the data as standalone calculations just so they finish
    activated_manager_name, _ = snowflake.activate_manager()
    socket = snowflake.get_storage_socket()
    run_optimization_test_data(socket, activated_manager_name, "opt_psi4_benzene")
    run_optimization_test_data(socket, activated_manager_name, "opt_psi4_methane")

    # Now load as a dataset entry/spec
    input_spec_1, molecule_1, _ = load_optimization_test_data("opt_psi4_benzene")
    input_spec_2, molecule_2, _ = load_optimization_test_data("opt_psi4_methane")
    molecule_3 = load_molecule_data("hooh")  # Won't be run

    opt_ds.add_entry(
        name="test_molecule_1", initial_molecule=molecule_1, comment="This is a comment", attributes={"key": "value"}
    )
    opt_ds.add_entry(name="test_molecule_2", initial_molecule=molecule_2)
    opt_ds.add_entry(name="test_molecule_3", initial_molecule=molecule_3)
    opt_ds.add_specification("test_spec", input_spec_1, "test_specification")

    opt_ds.submit()
    assert opt_ds.status()["test_spec"]["complete"] == 2
    assert opt_ds.status()["test_spec"]["waiting"] == 1

    # Now create the singlepoint dataset
    sp_ds = snowflake_client.add_dataset("singlepoint", "Test singlepoint dataset")
    sp_ds.add_entries_from(dataset_id=opt_ds.id, specification_name="test_spec")

    # refetch for up-to-date info
    sp_ds = snowflake_client.get_dataset_by_id(sp_ds.id)

    # Is missing molecule_3 - wasn't run
    assert set(sp_ds.entry_names) == {"test_molecule_1", "test_molecule_2"}

    sp_entry_1 = sp_ds.get_entry("test_molecule_1")
    sp_entry_2 = sp_ds.get_entry("test_molecule_2")
    opt_entry_1 = opt_ds.get_entry("test_molecule_1")
    opt_entry_2 = opt_ds.get_entry("test_molecule_2")
    opt_record_1 = opt_ds.get_record("test_molecule_1", "test_spec")
    opt_record_2 = opt_ds.get_record("test_molecule_2", "test_spec")

    assert opt_record_1.final_molecule_id == sp_entry_1.molecule.id
    assert opt_record_2.final_molecule_id == sp_entry_2.molecule.id

    assert opt_entry_1.comment == sp_entry_1.comment
    assert opt_entry_2.comment == sp_entry_2.comment

    assert opt_entry_1.attributes == sp_entry_1.attributes
    assert opt_entry_2.attributes == sp_entry_2.attributes

    # If we add again, nothing happens
    sp_ds.add_entries_from(dataset_id=opt_ds.id, specification_name="test_spec")

    sp_ds = snowflake_client.get_dataset_by_id(sp_ds.id)
    assert set(sp_ds.entry_names) == {"test_molecule_1", "test_molecule_2"}
    sp_entry_1 = sp_ds.get_entry("test_molecule_1")
    sp_entry_2 = sp_ds.get_entry("test_molecule_2")
    assert opt_record_1.final_molecule_id == sp_entry_1.molecule.id
    assert opt_record_2.final_molecule_id == sp_entry_2.molecule.id

    assert opt_entry_1.comment == sp_entry_1.comment
    assert opt_entry_2.comment == sp_entry_2.comment

    assert opt_entry_1.attributes == sp_entry_1.attributes
    assert opt_entry_2.attributes == sp_entry_2.attributes
