from sqlalchemy import union, DDL, event, table, Column, Integer

from qcfractal.components.gridoptimization.record_db_models import (
    record_children_select as gridoptimization_children,
    record_direct_children_select as gridoptimization_direct_children,
)
from qcfractal.components.manybody.record_db_models import (
    record_children_select as manybody_children,
    record_direct_children_select as manybody_direct_children,
)
from qcfractal.components.neb.record_db_models import (
    record_children_select as neb_children,
    record_direct_children_select as neb_direct_children,
)
from qcfractal.components.optimization.record_db_models import (
    record_children_select as optimization_children,
    record_direct_children_select as optimization_direct_children,
)
from qcfractal.components.reaction.record_db_models import (
    record_children_select as reaction_children,
    record_direct_children_select as reaction_direct_children,
)
from qcfractal.components.torsiondrive.record_db_models import (
    record_children_select as torsiondrive_children,
    record_direct_children_select as torsiondrive_direct_children,
)
from qcfractal.db_socket.base_orm import BaseORM
from qcfractal.db_socket.db_views import view

RecordDirectChildrenView = view(
    "record_direct_children_view",
    BaseORM.metadata,
    union(
        *optimization_direct_children,
        *gridoptimization_direct_children,
        *torsiondrive_direct_children,
        *manybody_direct_children,
        *reaction_direct_children,
        *neb_direct_children,
    ),
)


RecordChildrenView = view(
    "record_children_view",
    BaseORM.metadata,
    union(
        *optimization_children,
        *gridoptimization_children,
        *torsiondrive_children,
        *manybody_children,
        *reaction_children,
        *neb_children,
    ),
)
