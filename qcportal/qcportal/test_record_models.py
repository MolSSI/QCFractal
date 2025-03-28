from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.singlepoint.testing_helpers import (
    run_test_data as run_sp_test_data,
    load_test_data as load_sp_test_data,
    submit_test_data as submit_sp_test_data,
)
from qcfractal.components.torsiondrive.testing_helpers import submit_test_data as submit_td_test_data
from qcportal.compression import decompress
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake

all_includes = ["task", "service", "outputs", "comments", "native_files"]


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_base_record_model_common(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, _, result = load_sp_test_data("sp_psi4_h2_b3lyp_nativefiles")

    time_0 = now_at_utc()
    rec_id = run_sp_test_data(storage_socket, activated_manager_name, "sp_psi4_h2_b3lyp_nativefiles")
    time_1 = now_at_utc()

    snowflake_client.add_comment(rec_id, "This is a comment")
    time_2 = now_at_utc()

    record = snowflake_client.get_records(rec_id, include=includes)

    if includes is not None:
        assert record.native_files_ is not None
        assert record.comments_ is not None
        assert record.compute_history_ is not None
        record.propagate_client(None)
        assert record.offline
    else:
        assert record.native_files_ is None
        assert record.comments_ is None
        assert record.compute_history_ is None

    assert record.id == rec_id
    assert record.is_service is False

    assert record.status == RecordStatusEnum.complete

    assert time_0 < record.created_on < time_1
    assert time_0 < record.modified_on < time_1
    assert record.modified_on > record.created_on

    co = result.extras["_qcfractal_compressed_outputs"]["stdout"]
    ro = decompress(co["data"], co["compression_type"])
    assert record.stdout == ro

    assert len(record.comments) == 1
    assert record.comments[0].comment == "This is a comment"
    assert time_1 < record.comments[0].timestamp < time_2

    assert record.manager_name == activated_manager_name.fullname
    assert record.provenance
    assert record.provenance.creator

    assert len(record.compute_history) == 1
    assert time_0 < record.compute_history[0].modified_on < time_1
    assert record.compute_history[0].status == RecordStatusEnum.complete
    assert record.compute_history[0].manager_name == activated_manager_name.fullname
    assert record.compute_history[0].outputs["stdout"].data == ro


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_base_record_model_error(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    rec_id = run_sp_test_data(
        storage_socket, activated_manager_name, "sp_psi4_benzene_energy_1", end_status=RecordStatusEnum.error
    )

    record = snowflake_client.get_records(rec_id, include=includes)
    assert record.offline is False

    assert record.status == RecordStatusEnum.error
    assert record.error["error_type"] == "test_error"
    assert record.error["error_message"] == "this is just a test error"

    assert record.compute_history[0].status == RecordStatusEnum.error
    assert record.compute_history[0].manager_name == activated_manager_name.fullname
    err = record.compute_history[0].outputs["error"].data
    assert err["error_type"] == "test_error"
    assert err["error_message"] == "this is just a test error"


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_base_record_model_task(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    rec_id, _ = submit_sp_test_data(
        storage_socket, "sp_psi4_benzene_energy_1", compute_tag="test_tag_123", compute_priority=PriorityEnum.low
    )

    record = snowflake_client.get_records(rec_id, include=includes)

    if includes is not None:
        assert record.task_ is not None
        record.propagate_client(None)
        assert record.offline
    else:
        assert record.task_ is None

    assert record.manager_name is None
    assert record.task.compute_tag == "test_tag_123"
    assert record.task.compute_priority == PriorityEnum.low
    assert "psi4" in record.task.required_programs

    assert record.service is None


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_base_record_model_service(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    rec_id, _ = submit_td_test_data(
        storage_socket, "td_H2O2_mopac_pm6", compute_tag="test_tag_123", compute_priority=PriorityEnum.low
    )

    record = snowflake_client.get_records(rec_id, include=includes)

    if includes is not None:
        assert record.service_ is not None
        record.propagate_client(None)
        assert record.offline
    else:
        assert record.service_ is None

    assert record.is_service is True
    assert record.service.dependencies is not None
    assert record.manager_name is None
    assert record.service.compute_tag == "test_tag_123"
    assert record.service.compute_priority == PriorityEnum.low

    assert record.task is None
