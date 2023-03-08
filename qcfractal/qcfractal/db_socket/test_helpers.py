from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.components.optimization.record_db_models import OptimizationRecordORM
from qcfractal.components.optimization.testing_helpers import submit_test_data
from sqlalchemy import select
from qcfractal.db_socket.helpers import get_query_proj_options


def test_dbsocket_helper_proj(storage_socket: SQLAlchemySocket):
    empty_record_keys = {"id", "record_type"}

    record_id, _ = submit_test_data(storage_socket, "opt_psi4_methane_sometraj")

    with storage_socket.session_scope() as session:

        def run_proj(includes, excludes):
            stmt = select(OptimizationRecordORM).where(OptimizationRecordORM.id == record_id)
            query_opts = get_query_proj_options(OptimizationRecordORM, includes, excludes)
            stmt = stmt.options(*query_opts)
            d = session.execute(stmt).scalar_one().__dict__

            # remove sqlalchemy stuff
            return {k: v for k, v in d.items() if not k.startswith("_sa_")}

        # First, double check that some test columns are/aren't loaded by default)
        d = run_proj(None, None)
        assert "specification" in d
        assert "initial_molecule" not in d
        assert "trajectory" not in d
        assert "compute_history" not in d

        # default, no includes/excludes
        assert get_query_proj_options(OptimizationRecordORM) == []

        # explicit no includes/excludes
        assert get_query_proj_options(OptimizationRecordORM, None, None) == []

        # excludes is empty list
        assert get_query_proj_options(OptimizationRecordORM, None, []) == []

        # include only some columns
        d = run_proj(["initial_molecule_id"], None)
        assert "initial_molecule_id" in d
        assert "final_molecule_id" not in d
        assert "specification" not in d
        assert "specification_id" not in d

        # include only some relationships
        d = run_proj(["specification"], None)
        assert "initial_molecule_id" not in d
        assert "final_molecule_id" not in d
        assert "specification" in d
        assert "specification_id" not in d

        # include only some relationships not included by default (not allowed)
        d = run_proj(["initial_molecule", "compute_history"], None)
        assert "initial_molecule" not in d
        assert "compute_history" not in d
        assert "initial_molecule_id" not in d
        assert "final_molecule_id" not in d
        assert "specification" not in d
        assert "specification_id" not in d

        # include column + new rel + existing rel
        d = run_proj(["initial_molecule_id", "initial_molecule", "specification"], None)
        assert "initial_molecule" not in d  # new rel not allowed
        assert "initial_molecule_id" in d
        assert "final_molecule_id" not in d
        assert "specification" in d
        assert "specification_id" not in d

        #####################
        # Now some excludes
        #####################

        # Exclude columns and relationships that are loaded by default
        d = run_proj(None, ["initial_molecule_id", "specification"])
        assert "initial_molecule_id" not in d
        assert "specification" not in d
        assert "specification_id" in d

        d = run_proj(["*"], ["initial_molecule_id", "specification"])
        assert "initial_molecule_id" not in d
        assert "specification" not in d
        assert "specification_id" in d

        # Exclude columns and relationships that are not loaded by default
        assert get_query_proj_options(OptimizationRecordORM, None, ["initial_molecule"]) == []
        assert get_query_proj_options(OptimizationRecordORM, ["*"], ["initial_molecule"]) == []

        # Mixing includes & excludes (exclude overrides include)
        # Include/exclude column/relationships loaded by default
        d = run_proj(["specification", "initial_molecule_id"], ["specification", "initial_molecule_id"])
        assert d.keys() == empty_record_keys

        # Include/exclude column/relationships not loaded by default
        d = run_proj(["initial_molecule"], ["initial_molecule"])
        assert d.keys() == empty_record_keys

        # Handling non-existent columns/relationships
        d = run_proj(["not_a_column"], None)
        assert d.keys() == empty_record_keys

        d = run_proj(["not_a_column"], ["not_a_column"])
        assert d.keys() == empty_record_keys

        d = run_proj(["not_a_column", "initial_molecule_id"], ["not_a_column"])
        assert d.keys() == set(list(empty_record_keys) + ["initial_molecule_id"])

        assert get_query_proj_options(OptimizationRecordORM, None, ["not_a_column"]) == []

        # Empty queries
        # Note that id and record_type are always returned
        assert run_proj([], []).keys() == empty_record_keys

        assert run_proj(["initial_molecule_id"], ["initial_molecule_id"]).keys() == empty_record_keys

        # The only include is a relationship that can't be loaded
        assert run_proj(["initial_molecule"], None).keys() == empty_record_keys

        # Lastly, some complex examples
        d = run_proj(["specification", "initial_molecule_id", "compute_history"], ["specification_id"])
        assert "specification" in d
        assert "specification_id" not in d
        assert "initial_molecule_id" in d
        assert "status" not in d
        assert "compute_history" not in d
