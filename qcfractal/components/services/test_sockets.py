"""
Tests the services socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import json
import pytest

from qcfractal.components.records.optimization.db_models import OptimizationRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.portal.keywords import KeywordSet
from qcfractal.portal.molecules import Molecule
from qcfractal.portal.outputstore import OutputStore
from qcfractal.portal.managers import ManagerName
from qcfractal.portal.records import RecordStatusEnum, PriorityEnum
from qcfractal.portal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQueryBody,
    OptimizationSinglepointInputSpecification,
    OptimizationProtocols,
)
from qcfractal.portal.records.singlepoint import (
    SinglepointDriver,
    SinglepointProtocols,
)
from qcfractal.portal.records.torsiondrive import TorsiondriveInputSpecification, TorsiondriveKeywords
from qcfractal.testing import load_molecule_data, load_procedure_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_service_socket_fail(storage_socket: SQLAlchemySocket):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data("td_C7H8N2OS_psi4_fail")

    meta_1, id_1 = storage_socket.records.torsiondrive.add(input_spec_1, [molecules_1], as_service=True)
    assert meta_1.success
    rec = storage_socket.records.torsiondrive.get(id_1)
    assert rec[0]["status"] == RecordStatusEnum.waiting

    # A manager for completing the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={
            "geometric": None,
            "psi4": None,
        },
        tags=["*"],
    )

    time_0 = datetime.utcnow()
    r = storage_socket.services.iterate_services()
    time_1 = datetime.utcnow()

    while r > 0:
        rec = storage_socket.records.torsiondrive.get(
            id_1, include=["*", "service.*", "service.tasks.*", "service.tasks.record"]
        )

        assert rec[0]["status"] in {RecordStatusEnum.running, RecordStatusEnum.error}

        waiting_tasks = [x["record"] for x in rec[0]["service"]["tasks"]]
        assert len(waiting_tasks) > 0

        manager_tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=4)

        opt_ids = set(x["record_id"] for x in manager_tasks)
        opt_recs = storage_socket.records.optimization.get(opt_ids, include=["*", "initial_molecule", "task"])

        manager_ret = {}
        for opt in opt_recs:
            # Find out info about what tasks the service spawned
            mol_hash = opt["initial_molecule"]["identifiers"]["molecule_hash"]
            constraints = opt["specification"]["keywords"]["constraints"]

            # This is the key in the dictionary of optimization results
            optresult_key = mol_hash + "|" + json.dumps(constraints, sort_keys=True)
            opt_data = result_data_1[optresult_key]
            manager_ret[opt["task"]["id"]] = opt_data

        rmeta = storage_socket.tasks.update_finished(mname1.fullname, manager_ret)
        assert rmeta.n_accepted == len(manager_tasks)

        time_0 = datetime.utcnow()
        r = storage_socket.services.iterate_services()
        time_1 = datetime.utcnow()

    rec = storage_socket.records.torsiondrive.get(
        id_1, include=["*", "compute_history.*", "compute_history.outputs", "service"]
    )

    assert rec[0]["status"] == RecordStatusEnum.error
    assert rec[0]["service"] is not None
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.error
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is not None
