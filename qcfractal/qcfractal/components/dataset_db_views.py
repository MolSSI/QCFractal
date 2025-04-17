from sqlalchemy import union, select

from qcfractal.components.gridoptimization.dataset_db_models import (
    dataset_records_select as gridoptimization_dataset_records,
)
from qcfractal.components.manybody.dataset_db_models import dataset_records_select as manybody_dataset_records
from qcfractal.components.neb.dataset_db_models import dataset_records_select as neb_dataset_records
from qcfractal.components.optimization.dataset_db_models import dataset_records_select as optimization_dataset_records
from qcfractal.components.reaction.dataset_db_models import dataset_records_select as reaction_dataset_records
from qcfractal.components.record_db_views import RecordChildrenView
from qcfractal.components.singlepoint.dataset_db_models import dataset_records_select as singlepoint_dataset_records
from qcfractal.components.torsiondrive.dataset_db_models import dataset_records_select as torsiondrive_dataset_records
from qcfractal.db_socket.base_orm import BaseORM
from qcfractal.db_socket.db_views import view

DatasetDirectRecordsView = view(
    "dataset_direct_records_view",
    BaseORM.metadata,
    union(
        *singlepoint_dataset_records,
        *optimization_dataset_records,
        *gridoptimization_dataset_records,
        *torsiondrive_dataset_records,
        *manybody_dataset_records,
        *reaction_dataset_records,
        *neb_dataset_records,
    ),
)


DatasetRecordsView = view(
    "dataset_records_view",
    BaseORM.metadata,
    union(
        select(DatasetDirectRecordsView.c.dataset_id, DatasetDirectRecordsView.c.record_id),
        select(DatasetDirectRecordsView.c.dataset_id, RecordChildrenView.c.child_id.label("record_id")).join(
            RecordChildrenView, DatasetDirectRecordsView.c.record_id == RecordChildrenView.c.parent_id
        ),
    ),
)
