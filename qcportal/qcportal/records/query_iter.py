from __future__ import annotations

from typing import Tuple, Optional, List

from qcportal.base_models import QueryIteratorBase
from qcportal.metadata_models import QueryMetadata
from qcportal.records import RecordQueryFilters, BaseRecord
from qcportal.records.all_records import AllRecordDataModelTypes, records_from_datamodels


class RecordQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: RecordQueryFilters, record_type: Optional[str]):
        api_limit = client.api_limits["get_records"] // 4
        self.record_type = record_type

        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[BaseRecord]]:
        if self.record_type is None:
            meta, records = self.client._auto_request(
                "post",
                f"v1/records/query",
                type(self.query_filters),  # Pass through as is
                None,
                Tuple[Optional[QueryMetadata], List[AllRecordDataModelTypes]],
                self.query_filters,
                None,
            )
        else:
            meta, records = self.client._auto_request(
                "post",
                f"v1/records/{self.record_type}/query",
                type(self.query_filters),  # Pass through as is
                None,
                Tuple[Optional[QueryMetadata], List[AllRecordDataModelTypes]],
                self.query_filters,
                None,
            )

        return meta, records_from_datamodels(records, self.client)
