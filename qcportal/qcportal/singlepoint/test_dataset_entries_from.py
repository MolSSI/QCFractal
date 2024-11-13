from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.optimization.testing_helpers import (
    load_test_data as load_optimization_test_data,
    run_test_data as run_optimization_test_data,
)
from qcportal import PortalRequestError

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


@pytest.mark.parametrize("use_id", [True, False])
def test_singlepoint_dataset_model_entries_from_opt_1(snowflake: QCATestingSnowflake, use_id: bool):
    snowflake_client = snowflake.client()
    src_ds = snowflake_client.add_dataset("optimization", "Test src optimization dataset")

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

    src_ds.add_entry(
        name="test_molecule_1", initial_molecule=molecule_1, comment="This is a comment", attributes={"key": "value"}
    )
    src_ds.add_entry(name="test_molecule_2", initial_molecule=molecule_2)
    src_ds.add_entry(name="test_molecule_3", initial_molecule=molecule_3)
    src_ds.add_specification("test_spec", input_spec_1, "test_specification")

    src_ds.submit()
    assert src_ds.status()["test_spec"]["complete"] == 2
    assert src_ds.status()["test_spec"]["waiting"] == 1

    # Now create the singlepoint dataset
    sp_ds = snowflake_client.add_dataset("singlepoint", "Test singlepoint dataset")

    if use_id:
        m = sp_ds.add_entries_from(dataset_id=src_ds.id, specification_name="test_spec")
    else:
        # Purposefully change case of name - should be case insensitive
        m = sp_ds.add_entries_from(
            dataset_type="optimization", dataset_name=src_ds.name.upper(), specification_name="test_spec"
        )
    assert m.n_inserted == 2

    # refetch for up-to-date info
    sp_ds = snowflake_client.get_dataset_by_id(sp_ds.id)

    # Is missing molecule_3 - wasn't run
    assert set(sp_ds.entry_names) == {"test_molecule_1", "test_molecule_2"}

    sp_entry_1 = sp_ds.get_entry("test_molecule_1")
    sp_entry_2 = sp_ds.get_entry("test_molecule_2")
    opt_entry_1 = src_ds.get_entry("test_molecule_1")
    opt_entry_2 = src_ds.get_entry("test_molecule_2")
    opt_record_1 = src_ds.get_record("test_molecule_1", "test_spec")
    opt_record_2 = src_ds.get_record("test_molecule_2", "test_spec")

    assert opt_record_1.final_molecule_id == sp_entry_1.molecule.id
    assert opt_record_2.final_molecule_id == sp_entry_2.molecule.id

    assert opt_entry_1.comment == sp_entry_1.comment
    assert opt_entry_2.comment == sp_entry_2.comment

    assert opt_entry_1.attributes == sp_entry_1.attributes
    assert opt_entry_2.attributes == sp_entry_2.attributes

    # If we add again, nothing happens
    if use_id:
        m = sp_ds.add_entries_from(dataset_id=src_ds.id, specification_name="test_spec")
    else:
        m = sp_ds.add_entries_from(
            dataset_type="optimization", dataset_name=src_ds.name.upper(), specification_name="test_spec"
        )
    assert m.n_inserted == 0


@pytest.mark.parametrize("use_id", [True, False])
def test_singlepoint_dataset_model_entries_from_sp_1(snowflake: QCATestingSnowflake, use_id: bool):
    snowflake_client = snowflake.client()
    src_ds = snowflake_client.add_dataset("singlepoint", "Test src singlepoint dataset")

    # Now load as a dataset entry/spec
    molecule_1 = load_molecule_data("hooh")
    molecule_2 = load_molecule_data("neon_tetramer")
    molecule_3 = load_molecule_data("water_dimer_minima")

    src_ds.add_entry(
        name="test_molecule_1", molecule=molecule_1, comment="This is a comment", attributes={"key": "value"}
    )
    src_ds.add_entry(name="test_molecule_2", molecule=molecule_2)
    src_ds.add_entry(name="test_molecule_3", molecule=molecule_3)

    # Now create the singlepoint dataset
    sp_ds = snowflake_client.add_dataset("singlepoint", "Test singlepoint dataset")

    if use_id:
        m = sp_ds.add_entries_from(dataset_id=src_ds.id, specification_name="test_spec")
    else:
        # Purposefully change case of name - should be case insensitive
        m = sp_ds.add_entries_from(dataset_type="singlepoint", dataset_name=src_ds.name.upper())
    assert m.n_inserted == 3

    # refetch for up-to-date info
    sp_ds = snowflake_client.get_dataset_by_id(sp_ds.id)

    # Is missing molecule_3 - wasn't run
    assert set(sp_ds.entry_names) == {"test_molecule_1", "test_molecule_2", "test_molecule_3"}

    sp_entry_1 = sp_ds.get_entry("test_molecule_1")
    sp_entry_2 = sp_ds.get_entry("test_molecule_2")
    sp_entry_3 = sp_ds.get_entry("test_molecule_3")
    src_entry_1 = src_ds.get_entry("test_molecule_1")
    src_entry_2 = src_ds.get_entry("test_molecule_2")
    src_entry_3 = src_ds.get_entry("test_molecule_3")

    assert src_entry_1.molecule.id == sp_entry_1.molecule.id
    assert src_entry_2.molecule.id == sp_entry_2.molecule.id
    assert src_entry_3.molecule.id == sp_entry_3.molecule.id

    assert src_entry_1.comment == sp_entry_1.comment
    assert src_entry_2.comment == sp_entry_2.comment
    assert src_entry_3.comment == sp_entry_3.comment

    assert src_entry_1.attributes == sp_entry_1.attributes
    assert src_entry_2.attributes == sp_entry_2.attributes
    assert src_entry_3.attributes == sp_entry_3.attributes

    # If we add again, nothing happens
    if use_id:
        m = sp_ds.add_entries_from(dataset_id=src_ds.id, specification_name="test_spec")
    else:
        m = sp_ds.add_entries_from(dataset_type="singlepoint", dataset_name=src_ds.name.upper())
    assert m.n_inserted == 0


def test_singlepoint_dataset_model_entries_from_errors(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    src_opt_ds = snowflake_client.add_dataset("optimization", "Test src optimization dataset")
    src_sp_ds = snowflake_client.add_dataset("singlepoint", "Test src singlepoint dataset")

    sp_ds = snowflake_client.add_dataset("singlepoint", "Test singlepoint dataset")

    # Note - src_opt_ds.id + src_ds.id + sp_ds.id + 1 guaranteed not to exist
    missing_ds_id = src_opt_ds.id + src_sp_ds.id + sp_ds.id + 1

    # ------------------------------------ #
    # -- Validation errors caught early -- #
    # ------------------------------------ #

    # Missing both id and type
    with pytest.raises(ValueError, match="Either dataset_id or dataset_type and dataset_name"):
        sp_ds.add_entries_from(
            dataset_id=None, dataset_type=None, dataset_name=src_opt_ds.name, specification_name="test_spec"
        )

    # Source is optimization ds, but no specification given
    with pytest.raises(ValueError, match="specification_name must be given"):
        sp_ds.add_entries_from(dataset_id=src_opt_ds.id, dataset_type="optimization")

    # ------------------------ #
    # -- Server side errors -- #
    # ------------------------ #
    # Source dataset id does not exist
    with pytest.raises(PortalRequestError, match="Cannot find dataset"):
        sp_ds.add_entries_from(dataset_id=missing_ds_id, specification_name="test_spec")

    # Source dataset type/name does not exist
    with pytest.raises(PortalRequestError, match="Cannot find dataset"):
        sp_ds.add_entries_from(dataset_type="singlepoint", dataset_name="Does not exist")

    # Bad source dataset type for the given id
    with pytest.raises(PortalRequestError, match="not singlepoint"):
        sp_ds.add_entries_from(dataset_id=src_opt_ds.id, dataset_type="singlepoint", specification_name="test_spec")

    # Bad source dataset type for the given id
    with pytest.raises(PortalRequestError, match="not optimization"):
        sp_ds.add_entries_from(dataset_id=src_sp_ds.id, dataset_type="optimization", specification_name="test_spec")

    # Source is optimization ds, but no specification given
    with pytest.raises(PortalRequestError, match="from_specification_name must be provided"):
        sp_ds.add_entries_from(dataset_id=src_opt_ds.id)
