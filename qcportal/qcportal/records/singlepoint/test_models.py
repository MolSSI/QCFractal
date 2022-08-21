from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.records.singlepoint.testing_helpers import run_test_data, load_test_data
from qcportal.records import RecordStatusEnum
from .models import QCSpecification, SinglepointDriver

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


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


@pytest.mark.parametrize("includes", [None, all_includes])
def test_singlepointrecord_model(
    storage_socket: SQLAlchemySocket,
    snowflake_client: PortalClient,
    activated_manager_name: ManagerName,
    includes: Optional[List[str]],
):
    input_spec, molecule, result = load_test_data("sp_psi4_peroxide_energy_wfn")

    rec_id = run_test_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")
    record = snowflake_client.get_singlepoints(rec_id, include=includes)

    if includes is not None:
        record.client = None
        assert record.offline

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "singlepoint"
    assert record.specification == input_spec

    assert record.return_result == result.return_result
    assert record.properties.dict(encoding="json") == result.properties.dict(encoding="json")
    assert record.wavefunction.dict(encoding="json") == result.wavefunction.dict(encoding="json")
