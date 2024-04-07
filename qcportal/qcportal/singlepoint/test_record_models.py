from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.singlepoint.testing_helpers import run_test_data, load_test_data
from qcportal.record_models import RecordStatusEnum
from .record_models import QCSpecification, SinglepointDriver

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


all_includes = ["molecule", "wavefunction"]


def test_singlepoint_models_lowercase():
    s = QCSpecification(
        program="pROg1",
        driver=SinglepointDriver.energy,
        method="b3LYP",
        basis="def2-TZVP",
    )

    assert s.program == "prog1"
    assert s.method == "b3lyp"
    assert s.basis == "def2-tzvp"


def test_singlepoint_models_basis_convert():
    s = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="",
    )

    assert s.basis is None

    s = QCSpecification(
        program="prog1",
        driver="energy",
        method="b3lyp",
        basis=None,
    )

    assert s.basis is None


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_singlepoint_record_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, molecule, result = load_test_data("sp_psi4_peroxide_energy_wfn")

    rec_id = run_test_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")
    record = snowflake_client.get_singlepoints(rec_id, include=includes)

    if includes is not None:
        record.propagate_client(None)
        assert record.wavefunction_ is not None
        assert record.molecule_ is not None
        assert record.offline
    else:
        assert record.wavefunction_ is None
        assert record.molecule_ is None

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "singlepoint"
    assert record.specification == input_spec

    result_dict = result.dict(
        include={"return_result": True, "extras": {"qcvars"}, "properties": True}, encoding="json"
    )
    assert record.return_result == result_dict["return_result"]

    all_properties = result_dict["properties"]
    qcvars = result.extras.get("qcvars", {})
    all_properties.update({k.lower(): v for k, v in qcvars.items()})
    all_properties["return_result"] = result_dict["return_result"]
    assert record.properties == all_properties

    assert record.wavefunction.dict(encoding="json") == result.wavefunction.dict(encoding="json")
