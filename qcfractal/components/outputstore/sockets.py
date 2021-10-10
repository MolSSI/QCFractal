from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.db_socket.helpers import get_general
from qcfractal.exceptions import LimitExceededError, MissingDataError
from qcfractal.interface.models import OutputStore, CompressionEnum

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Optional, Sequence, Dict, Any, Union

    OutputDict = Dict[str, Any]


class OutputStoreSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.output_store

    @staticmethod
    def output_to_orm(output: OutputStore) -> OutputStoreORM:
        return OutputStoreORM(**output.dict())  # type: ignore

    def add(self, outputs: Sequence[Union[OutputStore, str, Dict]], *, session: Optional[Session] = None) -> List[int]:
        """
        Inserts output data into the database

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        outputs
            A sequence of output store to add. This can be a string or dictionary, which will be converted
            to a OutputStore object, or a OutputStore object itself
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            A list of all newly-inserted IDs
        """

        # Outputs are always added, so we don't use the general function

        # Since outputs can be relatively large, we don't explicitly create
        # a list of ORMs (one for each output). We only go one at a time

        output_ids = []

        with self.root_socket.optional_session(session) as session:
            for output in outputs:
                if isinstance(output, OutputStore):
                    kv_obj = output
                else:
                    kv_obj = OutputStore.compress(output, CompressionEnum.lzma, 1)

                output_orm = self.output_to_orm(kv_obj)
                session.add(output_orm)
                session.flush()
                output_ids.append(output_orm.id)

        return output_ids

    def get(
        self, id: Sequence[int], missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> List[Optional[OutputDict]]:
        """
        Obtain outputs with specified IDs

        The returned output information will be in the same order as the IDs specified with the `id` parameter.

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of OutputStore will be None.

        Parameters
        ----------
        id
            A list or other sequence of output ids
        missing_ok
           If set to True, then missing ids will be tolerated, and the returned list of
           OutputStore will contain None for the corresponding IDs that were not found.
           Otherwise, an exception will be raised.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Output information in the same order as the given ids. If missing_ok is True, then this list
            will contain None where the id was missing.
        """

        # Check that we are not requesting more outputs than the limit
        if len(id) > self._limit:
            raise LimitExceededError(f"Request for {len(id)} outputs is over the limit of {self._limit}")

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, OutputStoreORM, OutputStoreORM.id, id, None, None, None, missing_ok)

    def append(self, id: Optional[int], to_append: str, *, session: Optional[Session] = None) -> int:
        """
        Appends data to an output

        If the id is None, then one will be created.

        If the id does not exist, an exception will be raised

        Parameters
        ----------
        id
            The ID of the output to append data to
        to_append
            Data to append to the output
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            The ID of the output store. This is the same as the input if specified. If the input is None, then
            this will represent the ID of the new output object
        """

        with self.root_socket.optional_session(session) as session:
            if id is None:
                kv = OutputStore.compress(to_append)
                return self.add([kv], session=session)[0]
            else:
                output = session.query(OutputStoreORM).where(OutputStoreORM.id == id).one_or_none()

                if output is None:
                    raise MissingDataError(f"Cannot append to output {id} - does not exist!")

                kv = OutputStore(**output.dict())
                s = kv.get_string() + to_append
                new_orm = self.output_to_orm(OutputStore.compress(s, output.compression, output.compression_level))
                output.data = new_orm.data
                output.value = new_orm.value
                output.compression = new_orm.compression
                output.compression_level = new_orm.compression_level
                return output.id
