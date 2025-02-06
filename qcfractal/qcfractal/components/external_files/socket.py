from __future__ import annotations

import hashlib
import importlib
import logging
import os
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import select

from qcportal.exceptions import MissingDataError
from qcportal.external_files import ExternalFileTypeEnum, ExternalFileStatusEnum
from .db_models import ExternalFileORM

# Boto3 package is optional
_boto3_spec = importlib.util.find_spec("boto3")

if _boto3_spec is not None:
    boto3 = importlib.util.module_from_spec(_boto3_spec)
    _boto3_spec.loader.exec_module(boto3)


if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.components.internal_jobs.status import JobProgress
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Dict, Any, Tuple, Union, BinaryIO, Callable, Generator


class ExternalFileSocket:
    """
    Socket for managing/querying external files
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._s3_config = root_socket.qcf_config.s3

        if not self._s3_config.enabled:
            self._logger.info("S3 service for external files is not configured")
            return
        elif _boto3_spec is None:
            raise RuntimeError("boto3 package is required for S3 support")

        self._s3_client = boto3.client(
            "s3",
            endpoint_url=self._s3_config.endpoint_url,
            aws_access_key_id=self._s3_config.access_key_id,
            aws_secret_access_key=self._s3_config.secret_access_key,
            verify=self._s3_config.verify,
        )

        # may raise an exception (bad/missing credentials, etc)
        server_bucket_info = self._s3_client.list_buckets()
        server_buckets = {k["Name"] for k in server_bucket_info["Buckets"]}
        self._logger.info(f"Found {len(server_buckets)} buckets on the S3 server/account")

        # Make sure the buckets we use exist
        self._bucket_map = self._s3_config.bucket_map
        if self._bucket_map.dataset_attachment not in server_buckets:
            raise RuntimeError(f"Bucket {self._bucket_map.dataset_attachment} (for dataset attachments)")

    def _lookup_bucket(self, file_type: Union[ExternalFileTypeEnum, str]) -> str:
        # Can sometimes be a string from sqlalchemy (if set because of polymorphic identity)
        if isinstance(file_type, str):
            return getattr(self._bucket_map, file_type)
        elif isinstance(file_type, ExternalFileTypeEnum):
            return getattr(self._bucket_map, file_type.value)
        else:
            raise ValueError("Unknown parameter type for lookup_bucket: ", type(file_type))

    def add_data(
        self,
        file_data: BinaryIO,
        file_orm: ExternalFileORM,
        *,
        job_progress: Optional[JobProgress] = None,
        session: Optional[Session] = None,
    ) -> int:
        """
        Add raw file data to the database.

        This will fill in the appropriate fields on the given ORM object.
        The file_name and file_type must be filled in already.

        In this function, the `file_orm` will be added to the session and the session will be flushed.
        This is done at the beginning to show that the file is "processing", and at the end when the
        addition is completed.

        Parameters
        ----------
        file_data
            Binary data to be read from
        file_orm
            Existing ORM object that will be filled in with metadata
        job_progress
            Object used to track progress if this function is being run in a background job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            ID of the external file (which is also set in the given ORM object)
        """

        bucket = self._lookup_bucket(file_orm.file_type)

        self._logger.info(
            f"Uploading data to S3 bucket {bucket}. file_name={file_orm.file_name} type={file_orm.file_type}"
        )
        object_key = str(uuid.uuid4())
        sha256 = hashlib.sha256()
        file_size = 0

        multipart_upload = self._s3_client.create_multipart_upload(Bucket=bucket, Key=object_key)
        upload_id = multipart_upload["UploadId"]
        parts = []
        part_number = 1

        with self.root_socket.optional_session(session) as session:
            file_orm.status = ExternalFileStatusEnum.processing
            file_orm.bucket = bucket
            file_orm.object_key = object_key
            file_orm.sha256sum = ""
            file_orm.file_size = 0

            session.add(file_orm)
            session.flush()

            try:
                # Chunk size = 256MiB. There is a limit of 10,000 chunks, which would be
                # 2.56TiB
                while chunk := file_data.read(256 * 1024 * 1024):
                    if job_progress is not None:
                        job_progress.raise_if_cancelled()

                    sha256.update(chunk)
                    file_size += len(chunk)

                    response = self._s3_client.upload_part(
                        Bucket=bucket, Key=object_key, PartNumber=part_number, UploadId=upload_id, Body=chunk
                    )
                    parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                    part_number += 1

                self._s3_client.complete_multipart_upload(
                    Bucket=bucket, Key=object_key, UploadId=upload_id, MultipartUpload={"Parts": parts}
                )

            except Exception as e:
                self._s3_client.abort_multipart_upload(Bucket=bucket, Key=object_key, UploadId=upload_id)
                raise e

            self._logger.info(f"Uploading data to S3 bucket complete. Finishing writing metadata to db")
            file_orm.status = ExternalFileStatusEnum.available
            file_orm.sha256sum = sha256.hexdigest()
            file_orm.file_size = file_size
            session.flush()

            return file_orm.id

    def add_file(
        self,
        file_path: str,
        file_orm: ExternalFileORM,
        *,
        job_progress: Optional[JobProgress] = None,
        session: Optional[Session] = None,
    ) -> int:
        """
        Add an existing file to the database

        See documentation for :ref:`add_data` for more information.

        Parameters
        ----------
        file_path
            Path to an existing file to be read from
        file_orm
            Existing ORM object that will be filled in with metadata
        job_progress
            Object used to track progress if this function is being run in a background job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            ID of the external file (which is also set in the given ORM object)
        """

        file_size = os.path.getsize(file_path)
        self._logger.info(f"Uploading {file_path} to S3. File size: {file_size/1048576} MiB")

        with open(file_path, "rb") as f:
            return self.add_data(f, file_orm, job_progress=job_progress, session=session)

    def get_metadata(
        self,
        file_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Obtain external file information

        Parameters
        ----------
        file_id
            ID for the external file
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            List of molecule data (as dictionaries) in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        with self.root_socket.optional_session(session, True) as session:
            ef = session.get(ExternalFileORM, file_id)
            if ef is None:
                raise MissingDataError(f"Cannot find external file with id {file_id} in the database")

            return ef.model_dict()

    def delete(self, file_id: int, *, session: Optional[Session] = None):
        """
        Deletes an external file from the database and from remote storage

        Parameters
        ----------
        file_id
            ID of the external file to remove
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about what was deleted and any errors that occurred
        """

        with self.root_socket.optional_session(session) as session:
            stmt = select(ExternalFileORM).where(ExternalFileORM.id == file_id)
            ef_orm = session.execute(stmt).scalar_one_or_none()

            if ef_orm is None:
                raise MissingDataError(f"Cannot find external file with id {file_id} in the database")

            bucket = ef_orm.bucket
            object_key = ef_orm.object_key

            session.delete(ef_orm)
            session.flush()

            # now delete from S3 storage
            self._s3_client.delete_object(Bucket=bucket, Key=object_key)

    def get_url(self, file_id: int, *, session: Optional[Session] = None) -> Tuple[str, str]:
        """
        Obtain an url that a user can use to download the file directly from the S3 bucket

        Will raise an exception if the file_id does not exist

        Parameters
        ----------
        file_id
            ID of the external file
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            File name and direct URL to the file
        """

        with self.root_socket.optional_session(session, True) as session:
            ef = session.get(ExternalFileORM, file_id)
            if ef is None:
                raise MissingDataError(f"Cannot find external file with id {file_id} in the database")

            url = self._s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": ef.bucket,
                    "Key": ef.object_key,
                    "ResponseContentDisposition": f'attachment; filename = "{ef.file_name}"',
                },
                HttpMethod="GET",
                ExpiresIn=120,
            )

            return ef.file_name, url

    def get_file_streamer(
        self, file_id: int, *, session: Optional[Session] = None
    ) -> Tuple[str, Callable[[], Generator[bytes, None, None]]]:
        """
        Returns a function that streams a file from an S3 bucket.

        Parameters
        ----------
        file_id
            ID of the external file
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            The recommended filename and a generator function that yields chunks of the file
        """

        with self.root_socket.optional_session(session, True) as session:
            ef = session.get(ExternalFileORM, file_id)
            if ef is None:
                raise MissingDataError(f"Cannot find external file with id {file_id} in the database")

            s3_obj = self._s3_client.get_object(Bucket=ef.bucket, Key=ef.object_key)

            def _generator_func():
                for chunk in s3_obj["Body"].iter_chunks(chunk_size=(8 * 1048576)):
                    yield chunk

            return ef.file_name, _generator_func
