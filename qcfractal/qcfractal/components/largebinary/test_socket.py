from __future__ import annotations

from hashlib import md5
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.largebinary.db_models import LargeBinaryORM
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.compression import compress, CompressionEnum
from qcportal.exceptions import MissingDataError

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_largebinary_socket_basic(storage_socket: SQLAlchemySocket):

    lb_ids = []
    for compression_type in list(CompressionEnum):
        data = "This is some data" * 10000
        compressed, ctype, _ = compress(data, compression_type=compression_type)
        assert ctype == compression_type

        lb_orm = LargeBinaryORM()
        storage_socket.largebinary.populate_orm(lb_orm, compressed, compression_type)
        lb_id = storage_socket.largebinary.add_orm(lb_orm)
        assert lb_id is not None

        compressed2, compression_type2 = storage_socket.largebinary.get_raw(lb_id)
        assert compression_type2 == compression_type
        assert compressed2 == compressed

        lb_meta = storage_socket.largebinary.get_metadata(lb_id)
        assert lb_meta["id"] == lb_id
        assert lb_meta["size"] == len(compressed)
        assert lb_meta["compression_type"] == compression_type
        assert lb_meta["checksum"] == md5(compressed).hexdigest()
        assert set(lb_meta.keys()) == {"id", "size", "compression_type", "checksum"}

        lb_ids.append(lb_id)

    existing_ids = set(lb_ids)

    for lb_id in lb_ids:
        storage_socket.largebinary.delete(lb_id)

        with pytest.raises(MissingDataError, match="Cannot find large binary data"):
            storage_socket.largebinary.get_raw(lb_id)

        existing_ids.remove(lb_id)

        for exist_lb_id in existing_ids:
            # Just see if it exists
            storage_socket.largebinary.get_raw(exist_lb_id)
