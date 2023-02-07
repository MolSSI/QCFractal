from __future__ import annotations

import logging
from hashlib import md5
from typing import TYPE_CHECKING

from sqlalchemy import select, delete
from sqlalchemy.orm import load_only

from qcportal.compression import CompressionEnum, compress, decompress
from qcportal.exceptions import MissingDataError, CorruptDataError
from .db_models import LargeBinaryORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Tuple, Dict, Any


class LargeBinarySocket:
    """
    Socket for managing tasks
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self._tasks_claim_limit = root_socket.qcf_config.api_limits.manager_tasks_claim

    def add(self, data: bytes, compression_type: CompressionEnum, *, session: Optional[Session] = None) -> int:

        size = len(data)
        checksum = md5(data).hexdigest()

        orm = LargeBinaryORM(size=size, checksum=checksum, compression_type=compression_type, data_local=data)

        with self.root_socket.optional_session(session) as session:
            session.add(orm)
            session.flush()
            return orm.id

    def add_compress(
        self,
        data: Any,
        compression_type: CompressionEnum = CompressionEnum.zstd,
        *,
        session: Optional[Session] = None,
    ) -> int:

        data_compressed, _, _ = compress(data, compression_type=compression_type)
        return self.add(data_compressed, compression_type, session=session)

    def get_metadata(self, lb_id: int, *, session: Optional[Session] = None) -> Dict[str, Any]:

        stmt = select(LargeBinaryORM)
        stmt = stmt.options(
            load_only(LargeBinaryORM.id, LargeBinaryORM.size, LargeBinaryORM.checksum, LargeBinaryORM.compression_type)
        )
        stmt = stmt.where(LargeBinaryORM.id == lb_id)

        with self.root_socket.optional_session(session) as session:
            r = session.execute(stmt).scalar_one_or_none()

            if r is None:
                raise MissingDataError(f"Cannot find large binary data with id {lb_id}")

            return r.model_dict()

    def get_raw(
        self, lb_id: int, skip_checksum: bool = False, *, session: Optional[Session] = None
    ) -> Tuple[bytes, CompressionEnum]:

        if lb_id is None:
            raise RuntimeError(f"No id specified")

        stmt = select(LargeBinaryORM).where(LargeBinaryORM.id == lb_id)
        with self.root_socket.optional_session(session) as session:
            r = session.execute(stmt).scalar_one_or_none()

            if r is None:
                raise MissingDataError(f"Cannot find large binary data with id {lb_id}")

            if not skip_checksum:
                checksum = md5(r.data_local).hexdigest()
                if checksum != r.checksum:
                    raise CorruptDataError(
                        f"Possible data corruption. lb_id={lb_id} stored={r.checksum} calculated={checksum}"
                    )

            return r.data_local, r.compression_type

    def get(self, lb_id: int, skip_checksum: bool = False, *, session: Optional[Session] = None) -> Any:
        raw_data, compression_type = self.get_raw(lb_id, skip_checksum, session=session)
        return decompress(raw_data, compression_type)

    def delete(self, lb_id: int, *, session: Optional[Session] = None):

        stmt = delete(LargeBinaryORM).where(LargeBinaryORM.id == lb_id)

        with self.root_socket.optional_session(session) as session:
            session.execute(stmt)
