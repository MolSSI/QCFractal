from __future__ import annotations

import logging
import math
import os
from collections.abc import Collection, Iterable, Sequence
from datetime import datetime
from typing import Any, TypeVar, Literal, cast, overload

from tabulate import tabulate

from qcportal.cache import DatasetCache, read_dataset_metadata
from qcportal.external_files import ExternalFile
from qcportal.gridoptimization import (
    GridoptimizationKeywords,
    GridoptimizationAddBody,
    GridoptimizationRecord,
    GridoptimizationQueryFilters,
)
from qcportal.manybody import (
    BSSECorrectionEnum,
    ManybodyRecord,
    ManybodyAddBody,
    ManybodyKeywords,
    ManybodyQueryFilters,
)
from qcportal.neb import (
    NEBKeywords,
    NEBAddBody,
    NEBQueryFilters,
    NEBRecord,
)
from qcportal.optimization import (
    OptimizationProtocols,
    OptimizationRecord,
    OptimizationQueryFilters,
    OptimizationSpecification,
    OptimizationAddBody,
)
from qcportal.reaction import (
    ReactionAddBody,
    ReactionRecord,
    ReactionKeywords,
    ReactionQueryFilters,
)
from qcportal.services.models import (  # noqa
    ServiceSubtaskRecord,
)
from qcportal.singlepoint import (
    QCSpecification,
    SinglepointRecord,
    SinglepointAddBody,
    SinglepointQueryFilters,
    SinglepointDriver,
    SinglepointProtocols,
)
from qcportal.torsiondrive import (
    TorsiondriveKeywords,
    TorsiondriveAddBody,
    TorsiondriveRecord,
    TorsiondriveQueryFilters,
)
from .auth import (
    UserInfo,
    GroupInfo,
    APIToken,
    NewAPIToken,
    APITokenCreateBody,
    APITokenScopeEnum,
    is_valid_username,
    is_valid_password,
    is_valid_groupname,
)
from .base_models import CommonBulkGetNamesBody, CommonBulkGetBody
from .cache import PortalCache
from .client_base import PortalClientBase
from .dataset_models import (
    BaseDataset,
    DatasetQueryModel,
    DatasetQueryRecords,
    DatasetDeleteParams,
    DatasetCloneBody,
    DatasetAddBody,
    dataset_from_dict,
    load_dataset_view,  # noqa
    create_dataset_view,
)
from .internal_jobs import InternalJob, InternalJobQueryFilters, InternalJobQueryIterator, InternalJobStatusEnum
from .managers import ManagerQueryFilters, ManagerQueryIterator, ManagerQueryAvailableFilters, ComputeManager
from .metadata_models import UpdateMetadata, InsertMetadata, DeleteMetadata
from .molecules import (
    Molecule,
    MoleculeIdentifiers,
    MoleculeModifyBody,
    MoleculeQueryIterator,
    MoleculeQueryFilters,
    MoleculeUploadOptions,
)

from .project_models import (
    Project,
    ProjectAddBody,
    ProjectDeleteParams,
    ProjectQueryModel,
    ProjectQueryRecords,
    ProjectQueryDatasets,
)

from .record_models import (
    RecordStatusEnum,
    PriorityEnum,
    RecordQueryFilters,
    RecordModifyBody,
    RecordDeleteBody,
    RecordRevertBody,
    BaseRecord,
    RecordQueryIterator,
    records_from_dicts,
)
from .serverinfo import (
    AccessLogQueryFilters,
    AccessLogSummaryFilters,
    AccessLogSummaryEntry,
    AccessLogSummary,
    AccessLogQueryIterator,
    ErrorLogQueryFilters,
    ErrorLogQueryIterator,
    DeleteBeforeDateBody,
    ServerStatsEntry,
)
from .utils import make_list, is_scalar, chunk_iterable, process_chunk_iterable

_T = TypeVar("_T", bound=BaseRecord)


class PortalClient(PortalClientBase):
    """
    Main class for interacting with a QCArchive server
    """

    def __init__(
        self,
        address: str,
        username: str | None = None,
        password: str | None = None,
        verify: bool = True,
        show_motd: bool = True,
        *,
        api_token: str | None = None,
        cache_dir: str | None = None,
        cache_max_size: int = 0,
    ) -> None:
        """
        Parameters
        ----------
        address
            The host or IP address of the FractalServer instance, including protocol and port if necessary
            ("https://ml.qcarchive.molssi.org", "http://192.168.1.10:8888")
        username
            The username to authenticate with.
        password
            The password to authenticate with.
        verify
            Verifies the SSL connection with a third party server. This may be False if a
            FractalServer was not provided an SSL certificate and defaults back to self-signed
            SSL keys.
        show_motd
            If a Message-of-the-Day is available, display it
        api_token
            A long-lived API token to authenticate with, instead of a username and password
        cache_dir
            Directory to store an internal cache of records and other data
        cache_max_size
            Maximum size of the cache directory
        """

        PortalClientBase.__init__(self, address, username, password, verify, show_motd, api_token=api_token)
        self._logger = logging.getLogger("PortalClient")
        self.cache = PortalCache(address, cache_dir, cache_max_size)

    def __repr__(self) -> str:
        """A short representation of the current PortalClient.

        Returns
        -------
        str
            The desired representation.
        """

        # What if there was an error before we could get the server info?
        if self.server_info is None:
            server_name = "?"
        else:
            server_name = self.server_name

        return f"PortalClient(server_name='{server_name}', address='{self.address}', username='{self.username}')"

    def _repr_html_(self) -> str:
        """A representation of this client for display in Jupyter notebooks"""

        # What if there was an error before we could get the server info?
        if self.server_info is None:
            server_name = "?"
        else:
            server_name = self.server_name

        output = f"""
        <h3>PortalClient</h3>
        <ul>
          <li><b>Server:   &nbsp; </b>{server_name}</li>
          <li><b>Address:  &nbsp; </b>{self.address}</li>
          <li><b>Username: &nbsp; </b>{self.username}</li>
        </ul>
        """

        # postprocess due to raw spacing above
        return "\n".join([substr.strip() for substr in output.split("\n")])

    def get_server_information(self) -> dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        return self.make_request("get", "api/v1/information", dict[str, Any])

    def get_server_openapi_spec(self) -> dict[str, Any]:
        """Request the OpenAPI specification for the server

        Returns
        -------
        :
            OpenAPI specification.
        """

        return self.make_request("get", "api/v1/openapi_spec", dict[str, Any])

    def get_server_stats(self) -> list[ServerStatsEntry]:
        """Request statistics about the server

        Returns
        -------
        :
            Server statistics.
        """

        return self.make_request("get", "api/v1/server_stats", list[ServerStatsEntry])

    #################################################
    # Message-of-the-Day (MOTD)
    #################################################
    def get_motd(self) -> str:
        """
        Gets the Message-of-the-Day (MOTD) from the server

        Returns
        -------
        :
            The current Message-of-the-Day. May be an empty string if none is set
        """

        return self.make_request("get", "api/v1/motd", str)

    def set_motd(self, new_motd: str) -> None:
        """
        Sets the Message-of-the-Day (MOTD) on the server

        Parameters
        ----------
        new_motd
            The new Message-of-the-Day. Set to an empty string to remove the existing one
        """

        return self.make_request("put", "api/v1/motd", None, body=new_motd)

    ##############################################################
    # Projects
    ##############################################################
    def add_project(
        self,
        name: str,
        description: str | None = None,
        tagline: str | None = None,
        tags: list[str] | None = None,
        default_compute_tag: str = "*",
        default_compute_priority: PriorityEnum = PriorityEnum.normal,
        extras: dict[str, Any] | None = None,
        existing_ok: bool = False,
    ) -> Project:
        """
        Creates a new project on the server

        Project names are unique across the server, and are compared case-insensitively.

        Parameters
        ----------
        name
            Name of the new project
        description
            Optional longer description of the project
        tagline
            Optional short description of the project
        tags
            Optional list of tags to attach to the project
        default_compute_tag
            The default :ref:`compute tag <glossary_tag>` for computations created within this
            project. Records and datasets added to the project inherit this unless they override it
        default_compute_priority
            The default priority for computations created within this project
        extras
            Optional dictionary of arbitrary additional information
        existing_ok
            If True, return the existing project if one already exists with this name, rather
            than raising an exception

        Returns
        -------
        :
            The new project (or existing project if `existing_ok=True` and a project with the given
            name already exists)
        """
        if description is None:
            description = ""
        if tagline is None:
            tagline = ""
        if tags is None:
            tags = []
        if extras is None:
            extras = {}

        body = ProjectAddBody(
            name=name,
            description=description,
            tagline=tagline,
            tags=tags,
            default_compute_tag=default_compute_tag,
            default_compute_priority=default_compute_priority,
            extras=extras,
            existing_ok=existing_ok,
        )

        proj_id = self.make_request("post", f"api/v1/projects", int, body=body)
        return self.get_project_by_id(proj_id)

    def get_project(self, project_name: str) -> Project:
        """
        Obtain a project by name

        The name is matched case-insensitively.

        Parameters
        ----------
        project_name
            Name of the project to obtain

        Returns
        -------
        :
            The project with the given name
        """
        body = ProjectQueryModel(project_name=project_name)
        proj_dict = self.make_request("post", f"api/v1/projects/query", dict[str, Any], body=body)
        return Project(**proj_dict, client=self)

    def get_project_by_id(self, project_id: int) -> Project:
        """
        Obtain a project by ID

        Parameters
        ----------
        project_id
            ID of the project to obtain

        Returns
        -------
        :
            The project with the given ID
        """
        project_dict = self.make_request("get", f"api/v1/projects/{project_id}", dict[str, Any])
        return Project(**project_dict, client=self)

    def delete_project(
        self,
        project_id: int,
        delete_records: bool = False,
        delete_datasets: bool = False,
        delete_dataset_records: bool = False,
    ) -> None:
        """
        Deletes a project from the server

        By default, only the project itself is deleted. The records and datasets it contained
        remain on the server.

        Parameters
        ----------
        project_id
            ID of the project to delete
        delete_records
            If True, also delete the records that were added directly to the project
        delete_datasets
            If True, also delete the datasets that were in the project
        delete_dataset_records
            If True, also delete the records contained in those datasets
        """

        params = ProjectDeleteParams(
            delete_records=delete_records,
            delete_datasets=delete_datasets,
            delete_dataset_records=delete_dataset_records,
        )
        return self.make_request("delete", f"api/v1/projects/{project_id}", None, url_params=params)

    def list_projects(self) -> list[dict[str, Any]]:
        """
        Obtain a summary of all projects on the server

        Each entry is a dictionary with the keys ``id``, ``project_name``, ``tagline``, ``tags``,
        ``description``, ``record_count``, ``dataset_count``, ``owner_user``, and ``creator_user``.

        The full project is not returned - use :meth:`get_project` or :meth:`get_project_by_id`
        for that.

        Returns
        -------
        :
            A list of dictionaries, one per project, ordered by project ID
        """
        return self.make_request("get", f"api/v1/projects", list[dict[str, Any]])

    def query_project_records(self, record_id: int | Iterable[int]) -> list[dict[str, Any]]:
        """
        Determine which projects the given records belong to

        Records that are not in any project simply do not appear in the result, so the returned
        list may be shorter than the list of IDs given.

        Parameters
        ----------
        record_id
            A record ID, or a collection of record IDs, to look up

        Returns
        -------
        :
            A list of dictionaries, each with the keys ``record_id``, ``project_id``,
            ``project_name``, and ``record_name``
        """
        body = ProjectQueryRecords(record_id=make_list(record_id))
        return self.make_request("post", f"api/v1/projects/queryrecords", list[dict[str, Any]], body=body)

    def query_project_datasets(self, dataset_id: int | Iterable[int]) -> list[dict[str, Any]]:
        """
        Determine which projects the given datasets belong to

        Datasets that are not in any project simply do not appear in the result, so the returned
        list may be shorter than the list of IDs given.

        Parameters
        ----------
        dataset_id
            A dataset ID or list of dataset IDs to look up

        Returns
        -------
        :
            A list of dictionaries, each with the keys ``record_id`` (which holds the *dataset*
            ID), ``project_id``, ``project_name``, and ``dataset_name``
        """
        body = ProjectQueryDatasets(dataset_id=make_list(dataset_id))
        return self.make_request("post", f"api/v1/projects/querydatasets", list[dict[str, Any]], body=body)

    ##############################################################
    # Datasets
    ##############################################################
    def list_datasets(self) -> list[dict[str, Any]]:
        """
        Obtain a summary of all datasets on the server

        Returns
        -------
        :
            A list of dictionaries, one per dataset, containing basic information about each dataset
        """

        return self.make_request("get", f"api/v1/datasets", list[dict[str, Any]])

    def list_datasets_table(self) -> str:
        """
        Formats a summary of all datasets on the server as a table

        Returns
        -------
        :
            A table of the datasets on the server (id, type, record count, and name), as a string
            suitable for printing
        """

        ds_list = self.list_datasets()

        headers: list[str]
        table: list[tuple[Any, ...]]

        # Listing includes descriptions, but we don't put them in the table
        # older servers don't have record_count
        if all("record_count" in x for x in ds_list):
            headers = ["id", "type", "record_count", "name"]
            table = [(x["id"], x["dataset_type"], x["record_count"], x["dataset_name"]) for x in ds_list]
        else:
            headers = ["id", "type", "name"]
            table = [(x["id"], x["dataset_type"], x["dataset_name"]) for x in ds_list]

        return tabulate(table, headers=headers)

    def print_datasets_table(self) -> None:
        """
        Prints a summary of all datasets on the server as a table
        """

        print(self.list_datasets_table())

    def get_dataset(self, dataset_type: str, dataset_name: str) -> BaseDataset:
        """
        Obtain a dataset with the specified type and name

        Parameters
        ----------
        dataset_type
            Type of the dataset to obtain ("singlepoint", "optimization", ...)
        dataset_name
            Name of the dataset to obtain. The name is matched case-insensitively

        Returns
        -------
        :
            The dataset, as a subclass of :class:`~qcportal.dataset_models.BaseDataset` matching
            the type of the dataset
        """

        body = DatasetQueryModel(dataset_name=dataset_name, dataset_type=dataset_type)
        ds = self.make_request("post", f"api/v1/datasets/query", dict[str, Any], body=body)

        return dataset_from_dict(ds, self, "api/v1")

    def query_dataset_records(
        self,
        record_id: int | Iterable[int],
        dataset_type: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Determine which datasets the given records belong to

        Parameters
        ----------
        record_id
            A record ID, or a collection of record IDs, to look up
        dataset_type
            Only include datasets of these types in the result. If None, all types are included

        Returns
        -------
        :
            A list of dictionaries, each with information about the dataset a record belongs to
        """

        body = DatasetQueryRecords(record_id=make_list(record_id), dataset_type=make_list(dataset_type))
        return self.make_request("post", f"api/v1/datasets/queryrecords", list[dict[str, Any]], body=body)

    def get_dataset_by_id(self, dataset_id: int) -> BaseDataset:
        """
        Obtain a dataset with the specified ID

        Parameters
        ----------
        dataset_id
            ID of the dataset to obtain

        Returns
        -------
        :
            The dataset, as a subclass of :class:`~qcportal.dataset_models.BaseDataset` matching
            the type of the dataset
        """

        ds = self.make_request("get", f"api/v1/datasets/{dataset_id}", dict[str, Any])
        return dataset_from_dict(ds, self, "api/v1")

    def dataset_from_cache(self, file_path: str) -> BaseDataset:
        """
        Obtain a dataset from a local cache file

        The cache file must have been created while connected to the same server this client is
        connected to. If the dataset no longer exists on the server, the cache is marked read-only
        and the dataset can still be used offline.

        Parameters
        ----------
        file_path
            Full path to an existing dataset cache file (see :meth:`create_dataset_view`)

        Returns
        -------
        :
            The dataset stored in the cache file, attached to this client
        """

        ds_meta = read_dataset_metadata(file_path)
        ds_type = BaseDataset.get_subclass(ds_meta["dataset_type"])
        ds_cache = DatasetCache(file_path, False, ds_type)

        ds = dataset_from_dict(ds_meta, self, "api/v1", cache_data=ds_cache)

        # Check to make sure we are connected to the same server
        cache_address = ds_cache.get_metadata("client_address")
        if cache_address != self.address:
            raise RuntimeError(f"Cache file comes from {cache_address}, but currently connected to {self.address}")

        try:
            self.get_dataset_by_id(ds.id)
        except:
            self._logger.warning(f"Dataset {ds.id} could not be found on the server. Marking as read-only cache")
            ds_cache.read_only = True

        return ds

    def create_dataset_view(
        self, dataset_id: int, file_path: str, include: Iterable[str] | None = None, overwrite: bool = False
    ) -> None:
        """
        Downloads a dataset into a file that can be used offline

        The entire dataset (entries, specifications, and records) is downloaded, which may take
        a while for large datasets. The resulting file can be opened with
        :func:`~qcportal.dataset_models.load_dataset_view` or :meth:`dataset_from_cache`.

        Parameters
        ----------
        dataset_id
            ID of the dataset to download
        file_path
            Full path to the file to create (including filename)
        include
            Additional fields to include in the downloaded records
        overwrite
            If True, allow for overwriting an existing file. If False, and a file already exists at
            the given path, an exception is raised
        """

        return create_dataset_view(self, dataset_id, file_path, include, overwrite)

    def get_dataset_status_by_id(self, dataset_id: int) -> dict[str, dict[RecordStatusEnum, int]]:
        """
        Obtain the status of the records in a dataset

        Parameters
        ----------
        dataset_id
            ID of the dataset to obtain the status of

        Returns
        -------
        :
            A dictionary of specification name to a dictionary of record status to the number of
            records of the dataset with that status
        """

        return self.make_request("get", f"api/v1/datasets/{dataset_id}/status", dict[str, dict[RecordStatusEnum, int]])

    def add_dataset(
        self,
        dataset_type: str,
        name: str,
        description: str | None = None,
        tagline: str | None = None,
        tags: list[str] | None = None,
        group: str | None = None,
        provenance: dict[str, Any] | None = None,
        visibility: bool | None = None,
        default_compute_tag: str = "*",
        default_compute_priority: PriorityEnum = PriorityEnum.normal,
        extras: dict[str, Any] | None = None,
        owner_group: str | None = None,
        existing_ok: bool = False,
        **kwargs: Any,  # For deprecated parameters
    ) -> BaseDataset:
        """
        Adds a new dataset to the server

        Dataset names are unique for a given dataset type, and are compared case-insensitively.

        Parameters
        ----------
        dataset_type
            Type of the dataset to create ("singlepoint", "optimization", ...)
        name
            Name of the new dataset
        description
            Optional longer description of the dataset
        tagline
            Optional short description of the dataset
        tags
            Optional list of tags to attach to the dataset
        group
            Deprecated and unused
        provenance
            Optional dictionary describing where this dataset came from
        visibility
            Deprecated and unused
        default_compute_tag
            The default :ref:`compute tag <glossary_tag>` for computations submitted from this
            dataset
        default_compute_priority
            The default priority for computations submitted from this dataset
        extras
            Optional dictionary of arbitrary additional information
        owner_group
            Deprecated and unused
        existing_ok
            If True, return the existing dataset if one already exists with this type and name,
            rather than raising an exception

        Returns
        -------
        :
            The new dataset (or the existing dataset if `existing_ok=True` and one with the given
            type and name already exists)
        """

        # TODO - DEPRECATED - Remove eventually
        if "default_tag" in kwargs:
            self._logger.warning("'default_tag' is deprecated; use 'default_compute_tag' instead")
            default_compute_tag = kwargs["default_tag"]
        if "default_priority" in kwargs:
            self._logger.warning("'default_priority' is deprecated; use 'default_compute_priority' instead")
            default_compute_priority = kwargs["default_priority"]
        if group is not None:
            self._logger.warning(f"'group' parameter has been deprecated and will be removed in a future version")
        if visibility is not None:
            self._logger.warning(f"'visibility' parameter has been deprecated and will be removed in a future version")
        if owner_group is not None:
            self._logger.warning(f"'owner_group' parameter has been deprecated and will be removed in a future version")

        if description is None:
            description = ""
        if tagline is None:
            tagline = ""
        if tags is None:
            tags = []
        if provenance is None:
            provenance = {}
        if extras is None:
            extras = {}

        if "metadata" in kwargs:
            self._logger.warning(
                f"'metadata' parameter has been deprecated and will be removed in a future version. Use 'extras' instead"
            )
            extras.update(kwargs["metadata"])

        body = DatasetAddBody(
            name=name,
            description=description,
            tagline=tagline,
            tags=tags,
            provenance=provenance,
            default_compute_tag=default_compute_tag,
            default_compute_priority=default_compute_priority,
            extras=extras,
            existing_ok=existing_ok,
        )

        ds_id = self.make_request("post", f"api/v1/datasets/{dataset_type}", int, body=body)
        return self.get_dataset_by_id(ds_id)

    def delete_dataset(self, dataset_id: int, delete_records: bool) -> None:
        """
        Deletes a dataset from the server

        Parameters
        ----------
        dataset_id
            ID of the dataset to delete
        delete_records
            If True, also delete the records the dataset contains. Otherwise the records remain on
            the server, just not as part of a dataset
        """

        params = DatasetDeleteParams(delete_records=delete_records)
        return self.make_request("delete", f"api/v1/datasets/{dataset_id}", None, url_params=params)

    def clone_dataset(self, source_dataset_id: int, new_dataset_name: str) -> BaseDataset:
        """
        Creates a copy of an existing dataset

        The new dataset has the same entries, specifications, and records as the source dataset.
        The records themselves are not duplicated - both datasets refer to the same records.

        Parameters
        ----------
        source_dataset_id
            ID of the dataset to copy
        new_dataset_name
            Name to give the new dataset

        Returns
        -------
        :
            The newly-created dataset
        """

        body = DatasetCloneBody(source_dataset_id=source_dataset_id, new_dataset_name=new_dataset_name)
        new_id = self.make_request("post", f"api/v1/datasets/clone", int, body=body)
        return self.get_dataset_by_id(new_id)

    ##############################################################
    # External files
    ##############################################################
    def get_external_file_direct_link(self, file_id: int) -> str:
        """
        Obtain a URL for downloading an external file directly

        Depending on how the server stores the file, this may be a link to another service (cloud
        storage, for example) rather than to the server itself.

        Parameters
        ----------
        file_id
            ID of the file to obtain a link for

        Returns
        -------
        :
            A URL the file can be downloaded from
        """

        url = self.make_request("get", f"api/v1/external_files/{file_id}/direct_link", str)
        if url.startswith("/"):
            # What was returned is a path relative to the current server's address.
            # That means the file is being passed-through the server, and there's no other link
            url = url.lstrip("/")
            url = f"{self.address}{url}"

        return url

    def download_external_file(self, file_id: int, destination_path: str, overwrite: bool = False) -> tuple[int, str]:
        """
        Downloads an external file to the given path

        The file size and checksum will be checked against the metadata stored on the server

        Parameters
        ----------
        file_id
            ID of the file to obtain
        destination_path
            Full path to the destination file (including filename)
        overwrite
            If True, allow for overwriting an existing file. If False, and a file already exists at the given
            destination path, an exception will be raised.

        Returns
        -------
        :
            A tuple of file size and sha256 checksum.

        """
        meta_url = f"api/v1/external_files/{file_id}"
        download_url = f"api/v1/external_files/{file_id}/download"

        # Check for local file existence before doing any requests
        if os.path.exists(destination_path) and not overwrite:
            raise RuntimeError(f"File already exists at {destination_path}. To overwrite, use `overwrite=True`")

        # First, get the metadata
        file_info = self.make_request("get", meta_url, ExternalFile)

        # Now actually download the file
        file_size, file_sha256 = self.download_file(
            download_url, destination_path, overwrite=overwrite, expected_size=file_info.file_size, show_progress=True
        )

        if file_size != file_info.file_size:
            raise RuntimeError(f"Inconsistent file size. Expected {file_info.file_size}, got {file_size}")

        if file_sha256 != file_info.sha256sum:
            raise RuntimeError(f"Inconsistent file checksum. Expected {file_info.sha256sum}, got {file_sha256}")

        return file_size, file_sha256

    ##############################################################
    # Molecules
    ##############################################################

    @overload
    def get_molecules(self, molecule_ids: int, missing_ok: Literal[False] = False) -> Molecule: ...

    @overload
    def get_molecules(self, molecule_ids: int, missing_ok: bool) -> Molecule | None: ...

    @overload
    def get_molecules(self, molecule_ids: Collection[int], missing_ok: Literal[False] = False) -> list[Molecule]: ...

    @overload
    def get_molecules(self, molecule_ids: Collection[int], missing_ok: bool) -> list[Molecule | None]: ...

    def get_molecules(
        self,
        molecule_ids: int | Collection[int],
        missing_ok: bool = False,
    ) -> Molecule | list[Molecule] | list[Molecule | None] | None:
        """Obtains molecules with the specified IDs from the server

        Parameters
        ----------
        molecule_ids
            A single molecule ID, or a collection of molecule IDs (list, tuple, set, numpy array, ...)
        missing_ok
            If set to True, then missing molecules will be tolerated, and the returned list of
            Molecules will contain None for the corresponding IDs that were not found.

        Returns
        -------
        :
            Molecules, in the same order as the requested ids. If given a collection of ids, the
            return value will be a list. Otherwise, it will be a single Molecule. Note that if the
            ids are given as an unordered collection (a set, for example), the order of the results
            is however that collection itself iterates.
        """

        is_single = is_scalar(molecule_ids)

        molecule_id_lst = make_list(molecule_ids)
        if not molecule_id_lst:
            return []

        batch_size = self.api_limits["get_molecules"] // 4
        all_molecules: list[Molecule | None] = []

        for mol_id_batch in chunk_iterable(molecule_id_lst, batch_size):
            body = CommonBulkGetBody(ids=mol_id_batch, missing_ok=missing_ok)
            mol_batch = self.make_request("post", "api/v1/molecules/bulkGet", list[Molecule | None], body=body)
            all_molecules.extend(mol_batch)

        if is_single:
            return all_molecules[0]
        else:
            return all_molecules

    def query_molecules(
        self,
        *,
        molecule_hash: str | Iterable[str] | None = None,
        molecular_formula: str | Iterable[str] | None = None,
        identifiers: dict[str, str | Iterable[str]] | None = None,
        limit: int | None = None,
    ) -> MoleculeQueryIterator:
        """Query molecules by attributes.

        Do not rely on the returned molecules being in any particular order.

        Parameters
        ----------
        molecule_hash
            Queries molecules by hash
        molecular_formula
            Queries molecules by molecular formula.
            Molecular formulas are not order-sensitive (e.g. "H2O == OH2 != Oh2").
        identifiers
            Additional identifiers to search for (smiles, etc)
        limit
            The maximum number of Molecules to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "molecule_hash": make_list(molecule_hash),
            "molecular_formula": make_list(molecular_formula),
            "limit": limit,
        }

        if identifiers is not None:
            filter_dict["identifiers"] = {k: make_list(v) for k, v in identifiers.items()}

        filter_data = MoleculeQueryFilters(**filter_dict)
        return MoleculeQueryIterator(self, filter_data)

    def add_molecules(self, molecules: Sequence[Molecule]) -> tuple[InsertMetadata, list[int]]:
        """Add molecules to the server database

        If the same molecule (defined by having the same hash) already exists, then the existing
        molecule is kept and that particular molecule is not added.

        Parameters
        ----------
        molecules
            A list of Molecules to add to the server.

        Returns
        -------
        :
            Metadata about what was inserted, and a list of IDs of the molecules
            in the same order as the `molecules` parameter.
        """

        if not molecules:
            return InsertMetadata(), []

        if len(molecules) > self.api_limits["add_molecules"]:
            raise RuntimeError(
                f"Cannot add {len(molecules)} molecules - over the limit of {self.api_limits['add_molecules']}"
            )

        mols = self.make_request(
            "post", "api/v1/molecules/bulkCreate", tuple[InsertMetadata, list[int]], body=make_list(molecules)
        )
        return mols

    def upload_molecules(self, file_paths: Sequence[str]) -> tuple[dict[str, list[tuple[str, int]]], list[str]]:
        """
        Adds molecules to the server by uploading files containing them

        A file may contain more than one molecule, and archives (zip, tar, tar.gz, ...) of such
        files may be uploaded as well. The file type is determined from the file extension.

        As with :meth:`add_molecules`, molecules that already exist on the server are not added
        again - the existing molecule id is returned instead.

        Parameters
        ----------
        file_paths
            Full paths of the files to upload

        Returns
        -------
        :
            A tuple of results and errors. The results map each uploaded file name to a list of
            the molecules obtained from it, as (name of the molecule within the file, molecule id)
            pairs. The errors are descriptions of any files (or molecules within a file) that
            could not be processed
        """

        file_info = [(os.path.basename(f), f) for f in file_paths]

        body = MoleculeUploadOptions()

        return self.make_request(
            "post",
            "api/v1/molecules/fromFiles",
            tuple[dict[str, list[tuple[str, int]]], list[str]],
            body=body,
            upload_files=file_info,
        )

    def modify_molecule(
        self,
        molecule_id: int,
        name: str | None = None,
        comment: str | None = None,
        identifiers: dict[str, Any] | MoleculeIdentifiers | None = None,
        overwrite_identifiers: bool = False,
    ) -> UpdateMetadata:
        """
        Modify molecules on the server

        This is only capable of updating the name, comment, and identifiers fields (except molecule_hash
        and molecular formula).

        If a molecule with that id does not exist, an exception is raised

        Parameters
        ----------
        molecule_id
            ID of the molecule to modify
        name
            New name for the molecule. If None, name is not changed.
        comment
            New comment for the molecule. If None, comment is not changed
        identifiers
            A new set of identifiers for the molecule
        overwrite_identifiers
            If True, the identifiers of the molecule are set to be those given exactly (ie, identifiers
            that exist in the DB but not in the new set will be removed). Otherwise, the new set of
            identifiers is merged into the existing ones. Note that molecule_hash and molecular_formula
            are never removed.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        body = MoleculeModifyBody(
            name=name, comment=comment, identifiers=identifiers, overwrite_identifiers=overwrite_identifiers
        )

        return self.make_request("patch", f"api/v1/molecules/{molecule_id}", UpdateMetadata, body=body)

    def delete_molecules(self, molecule_ids: int | Collection[int]) -> DeleteMetadata:
        """Deletes molecules from the server

        This will not delete any molecules that are in use

        Parameters
        ----------
        molecule_ids
            A single molecule ID, or a collection of molecule IDs (list, tuple, set, numpy array, ...)

        Returns
        -------
        :
            Metadata about what was deleted. The indices it contains refer to positions in the molecule
            ids given - if those were given as an unordered collection (a set, for example), those
            positions follow however that collection iterates.
        """

        molecule_ids = make_list(molecule_ids)
        if not molecule_ids:
            return DeleteMetadata()

        return self.make_request("post", "api/v1/molecules/bulkDelete", DeleteMetadata, body=molecule_ids)

    ##############################################################
    # General record functions
    ##############################################################

    def _fetch_records(
        self,
        base_url_prefix: str,
        record_type: type[_T] | None,
        record_ids: Sequence[int],
        missing_ok: bool = False,
        include: Iterable[str] | None = None,
    ) -> list[_T | None]:
        """
        Fetches records of a particular type with the specified IDs from the remove server.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case. This function always returns a list.

        This function only fetches the top-level records - it does not fetch the children of the records. It also
        does not use caching at all.

        Parameters
        ----------
        base_url_prefix
            Prefix of all the URLs for fetching records
        record_type
            The type of record to fetch
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        if not record_ids:
            return []

        if include is not None:
            # Always include the base stuff
            include = list(include) + ["*"]

        if record_type is None:
            endpoint = f"{base_url_prefix}/records/bulkGet"
        else:
            record_type_str = record_type.model_fields["record_type"].default
            endpoint = f"{base_url_prefix}/records/{record_type_str}/bulkGet"

        max_batch_size = self.api_limits["get_records"]
        initial_batch_size = math.ceil(max_batch_size // 10)

        def _download_chunk(id_chunk: list[int]) -> list[dict[str, Any] | None]:
            body = CommonBulkGetBody(ids=id_chunk, include=include, missing_ok=missing_ok)
            return self.make_request("post", endpoint, list[dict[str, Any] | None], body=body)

        all_records: list[_T | None] = []
        for record_dicts in process_chunk_iterable(
            _download_chunk,
            record_ids,
            self.download_target_time,
            max_batch_size,
            initial_batch_size,
            self.n_download_threads,
            keep_order=True,
        ):
            if record_type is None:
                # Without a record type, we get back whatever type each record happens to be
                all_records.extend(cast(list[_T | None], records_from_dicts(record_dicts, self, base_url_prefix)))
            else:
                all_records.extend(
                    [record_type(self, base_url_prefix, **r) if r is not None else None for r in record_dicts]
                )

        # Just to really make sure the process_chunk_iterable code is correct
        assert all((x is None or x.id == rid) for x, rid in zip(all_records, record_ids))
        return all_records

    @overload
    def get_records(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> BaseRecord: ...

    @overload
    def get_records(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> BaseRecord | None: ...

    @overload
    def get_records(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[BaseRecord]: ...

    @overload
    def get_records(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[BaseRecord | None]: ...

    def get_records(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> BaseRecord | list[BaseRecord] | list[BaseRecord | None] | None:
        """
        Obtain records of all types with specified IDs

        This function will return record objects of the given ID no matter
        what the type is. All records are unique by ID (ie, an optimization will never
        have the same ID as a singlepoint).

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", None, record_ids, missing_ok, include)

    def _get_records_by_type(
        self,
        base_url_prefix: str,
        record_type: type[_T] | None,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        include: Iterable[str] | None = None,
    ) -> _T | list[_T | None] | None:
        """
        Obtain records of a particular type with the specified IDs from the server.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        This function will fetch the children of the records if enough information
        is fetched of the parent record. This is handled by the various fetch_children_multi
        class functions of the record types.

        This function does not use the cache.

        Parameters
        ----------
        base_url_prefix
            Prefix of all the URLs for fetching records
        record_type
            The type of record to fetch
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = is_scalar(record_ids)

        record_id_lst = make_list(record_ids)
        all_records = self._fetch_records(base_url_prefix, record_type, record_id_lst, missing_ok, include)

        # We always force fetch here. Given that this record is not part of the cache, it shouldn't be using any
        # cache anyway. But the semantics of this function is that is always fetches everything
        if record_type is None:
            # Handle disparate record types
            record_groups: dict[str, list[_T]] = {}
            for r in all_records:
                if r is not None:
                    record_groups.setdefault(r.record_type, [])
                    record_groups[r.record_type].append(r)
            for v in record_groups.values():
                v[0].fetch_children_multi(v, include, force_fetch=True)
        else:
            record_type.fetch_children_multi(all_records, include, force_fetch=True)

        if is_single:
            return all_records[0]
        else:
            return all_records

    def query_records(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        record_type: str | Iterable[str] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        child_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[BaseRecord]:
        """
        Query records of all types based on common fields

        This is a general query of all record types, so it can only filter by fields
        that are common among all records.

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        record_type
            Query records whose type is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        creator_user
            Query records created by a user in the given list (usernames or IDs)
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "record_type": make_list(record_type),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = RecordQueryFilters(**filter_dict)

        return RecordQueryIterator[BaseRecord](self, filter_data, None, include)

    def reset_records(self, record_ids: int | Collection[int]) -> UpdateMetadata:
        """
        Resets running or errored records to be waiting again

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.waiting)
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

    def cancel_records(self, record_ids: int | Collection[int]) -> UpdateMetadata:
        """
        Marks running, waiting, or errored records as cancelled

        A cancelled record will not be picked up by a manager.

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.cancelled)
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

    def invalidate_records(self, record_ids: int | Collection[int]) -> UpdateMetadata:
        """
        Marks a completed record as invalid

        An invalid record is one that supposedly successfully completed. However, after review,
        is not correct.

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.invalid)
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

    def delete_records(
        self, record_ids: int | Collection[int], soft_delete: bool = True, delete_children: bool = True
    ) -> DeleteMetadata:
        """
        Delete records from the database

        If soft_delete is True, then the record is just marked as deleted and actually deletion may
        happen later. Soft delete can be undone with undelete

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)
        soft_delete
            Don't actually delete the record, just mark it for later deletion
        delete_children
            If True, attempt to delete child records as well

        Returns
        -------
        :
            Metadata about what was deleted. The indices it contains refer to positions in the record
            ids given - if those were given as an unordered collection (a set, for example), those
            positions follow however that collection iterates.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return DeleteMetadata()

        body = RecordDeleteBody(record_ids=record_ids, soft_delete=soft_delete, delete_children=delete_children)
        return self.make_request("post", "api/v1/records/bulkDelete", DeleteMetadata, body=body)

    def uninvalidate_records(self, record_ids: int | Collection[int]) -> UpdateMetadata:
        """
        Undo the invalidation of records

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.invalid)
        return self.make_request("post", "api/v1/records/revert", UpdateMetadata, body=body)

    def uncancel_records(self, record_ids: int | Collection[int]) -> UpdateMetadata:
        """
        Undo the cancellation of records

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.cancelled)
        return self.make_request("post", "api/v1/records/revert", UpdateMetadata, body=body)

    def undelete_records(self, record_ids: int | Collection[int]) -> UpdateMetadata:
        """
        Undo the (soft) deletion of records

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.deleted)
        return self.make_request("post", "api/v1/records/revert", UpdateMetadata, body=body)

    def modify_records(
        self,
        record_ids: int | Collection[int],
        new_compute_tag: str | None = None,
        new_compute_priority: PriorityEnum | None = None,
        **kwargs: Any,  # For deprecated parameters
    ) -> UpdateMetadata:
        """
        Modify the compute tag or compute priority of a record

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)
        new_compute_tag
            The new compute tag for the records. If None, the tag is not changed
        new_compute_priority
            The new compute priority for the records. If None, the priority is not changed

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """

        if "new_tag" in kwargs:
            self._logger.warning("'new_tag' is deprecated; use 'new_compute_tag' instead")
            new_compute_tag = kwargs["new_tag"]
        if "new_priority" in kwargs:
            self._logger.warning("'new_priority' is deprecated; use 'new_compute_priority' instead")
            new_compute_priority = kwargs["new_priority"]

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()
        if new_compute_tag is None and new_compute_priority is None:
            return UpdateMetadata()

        body = RecordModifyBody(
            record_ids=record_ids, compute_tag=new_compute_tag, compute_priority=new_compute_priority
        )
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

    def add_comment(self, record_ids: int | Collection[int], comment: str) -> UpdateMetadata:
        """
        Adds a comment to records

        Parameters
        ----------
        record_ids
            A single record ID, or a collection of record IDs (list, tuple, set, numpy array, ...)

        comment
            The comment string to add. Your username will be added automatically

        Returns
        -------
        :
            Metadata about which records were updated. The indices it contains refer to positions in
            the record ids given - if those were given as an unordered collection (a set, for example),
            those positions follow however that collection iterates.
        """
        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordModifyBody(record_ids=record_ids, comment=comment)
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body_data)

    def get_waiting_reason(self, record_id: int) -> dict[str, Any]:
        """
        Get the reason a record is in the waiting status

        The return is a dictionary, with a 'reason' key containing the overall reason the record is
        waiting. If appropriate, there is a 'details' key that contains information for each
        active compute manager on why that manager is not able to pick up the record's task.

        Parameters
        ----------
        record_id
            The record ID to test

        Returns
        -------
        :
            A dictionary containing information about why the record is not being picked up by compute managers
        """
        return self.make_request("get", f"api/v1/records/{record_id}/waiting_reason", dict[str, Any])

    ##############################################################
    # Singlepoint calculations
    ##############################################################

    def add_singlepoints(
        self,
        molecules: int | Molecule | Sequence[int | Molecule],
        program: str,
        driver: SinglepointDriver,
        method: str,
        basis: str | None,
        keywords: dict[str, Any] | None = None,
        protocols: SinglepointProtocols | dict[str, Any] | None = None,
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        find_existing: bool = True,
        **kwargs: Any,  # For deprecated parameters
    ) -> tuple[InsertMetadata, list[int]]:
        """
        Adds new singlepoint computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per molecule.

        Parameters
        ----------
        molecules
            The Molecules or Molecule ids to compute with the above methods
        program
            The computational program to execute the result with (e.g., "rdkit", "psi4").
        driver
            The primary result that the compute will acquire {"energy", "gradient", "hessian", "properties"}
        method
            The computational method to use (e.g., "B3LYP", "PBE")
        basis
            The basis to apply to the computation (e.g., "cc-pVDZ", "6-31G")
        keywords
            The program-specific keywords for the computation
        protocols
            Protocols for storing more/less data for each computation
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        molecules = make_list(molecules)
        if not molecules:
            return InsertMetadata(), []

        if len(molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data: dict[str, Any] = {
            "molecules": molecules,
            "specification": {
                "program": program,
                "driver": driver,
                "method": method,
                "basis": basis,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "find_existing": find_existing,
        }

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        body = SinglepointAddBody(**body_data)
        return self.make_request(
            "post", "api/v1/records/singlepoint/bulkCreate", tuple[InsertMetadata, list[int]], body=body
        )

    @overload
    def get_singlepoints(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> SinglepointRecord: ...

    @overload
    def get_singlepoints(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> SinglepointRecord | None: ...

    @overload
    def get_singlepoints(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[SinglepointRecord]: ...

    @overload
    def get_singlepoints(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[SinglepointRecord | None]: ...

    def get_singlepoints(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> SinglepointRecord | list[SinglepointRecord] | list[SinglepointRecord | None] | None:
        """
        Obtain singlepoint records with the specified IDs.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", SinglepointRecord, record_ids, missing_ok, include)

    def query_singlepoints(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        program: str | Iterable[str] | None = None,
        driver: SinglepointDriver | Iterable[SinglepointDriver] | None = None,
        method: str | Iterable[str] | None = None,
        basis: str | Iterable[str | None] | None = None,
        keywords: dict[str, Any] | Iterable[dict[str, Any]] | None = None,
        molecule_id: int | Iterable[int] | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[SinglepointRecord]:
        """
        Queries singlepoint records on the server

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose program is in the given list
        driver
            Query records whose driver is in the given list
        method
            Query records whose method is in the given list
        basis
            Query records whose basis is in the given list
        keywords
            Query records with these keywords (exact match)
        molecule_id
            Query records whose molecule (id) is in the given list
        creator_user
            Query records created by a user in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        # Note - singlepoints don't have any children
        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "program": make_list(program),
            "driver": make_list(driver),
            "method": make_list(method),
            "basis": make_list(basis),
            "keywords": make_list(keywords),
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = SinglepointQueryFilters(**filter_dict)
        return RecordQueryIterator[SinglepointRecord](self, filter_data, SinglepointRecord, include)

    ##############################################################
    # Optimization calculations
    ##############################################################

    def add_optimizations(
        self,
        initial_molecules: int | Molecule | Sequence[int | Molecule],
        program: str,
        qc_specification: QCSpecification,
        keywords: dict[str, Any] | None = None,
        protocols: OptimizationProtocols | None = None,
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        find_existing: bool = True,
        **kwargs: Any,  # For deprecated parameters
    ) -> tuple[InsertMetadata, list[int]]:
        """
        Adds new geometry optimization calculations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per initial molecule.

        Parameters
        ----------
        initial_molecules
            Initial molecule/geometry to optimize
        program
            Which program to use for the optimization (ie, geometric)
        qc_specification
            The method, basis, etc, to optimize the geometry with
        keywords
            Program-specific keywords for the optimization program (not the qc program)
        protocols
            Protocols for storing more/less data for each computation (for the optimization)
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data: dict[str, Any] = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "qc_specification": qc_specification,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "find_existing": find_existing,
        }

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        body = OptimizationAddBody(**body_data)

        return self.make_request(
            "post",
            "api/v1/records/optimization/bulkCreate",
            tuple[InsertMetadata, list[int]],
            body=body,
        )

    @overload
    def get_optimizations(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> OptimizationRecord: ...

    @overload
    def get_optimizations(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> OptimizationRecord | None: ...

    @overload
    def get_optimizations(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[OptimizationRecord]: ...

    @overload
    def get_optimizations(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[OptimizationRecord | None]: ...

    def get_optimizations(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> OptimizationRecord | list[OptimizationRecord] | list[OptimizationRecord | None] | None:
        """
        Obtain optimization records with the specified IDs.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", OptimizationRecord, record_ids, missing_ok, include)

    def query_optimizations(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        child_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        program: str | Iterable[str] | None = None,
        qc_program: str | Iterable[str] | None = None,
        qc_method: str | Iterable[str] | None = None,
        qc_basis: str | Iterable[str | None] | None = None,
        initial_molecule_id: int | Iterable[int] | None = None,
        final_molecule_id: int | Iterable[int] | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[OptimizationRecord]:
        """
        Queries optimization records on the server

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (singlepoint calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        initial_molecule_id
            Query records whose initial molecule (id) is in the given list
        final_molecule_id
            Query records whose final molecule (id) is in the given list
        creator_user
            Query records created by a user in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "final_molecule_id": make_list(final_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = OptimizationQueryFilters(**filter_dict)
        return RecordQueryIterator[OptimizationRecord](self, filter_data, OptimizationRecord, include)

    ##############################################################
    # Torsiondrive calculations
    ##############################################################

    def add_torsiondrives(
        self,
        initial_molecules: Sequence[Sequence[int | Molecule]],
        program: str,
        optimization_specification: OptimizationSpecification,
        keywords: TorsiondriveKeywords | dict[str, Any],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        find_existing: bool = True,
        **kwargs: Any,  # For deprecated parameters
    ) -> tuple[InsertMetadata, list[int]]:
        """
        Adds new torsiondrive computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per set of molecules

        Parameters
        ----------
        initial_molecules
            Molecules to start the torsiondrives. Each torsiondrive can start with
            multiple molecules, so this is a nested list
        program
            The program to run the torsiondrive computation with ("torsiondrive")
        optimization_specification
            Specification of how each optimization of the torsiondrive should be run
        keywords
            The torsiondrive keywords for the computation
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data: dict[str, Any] = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "as_service": True,
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "find_existing": find_existing,
        }

        body = TorsiondriveAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/torsiondrive/bulkCreate", tuple[InsertMetadata, list[int]], body=body
        )

    @overload
    def get_torsiondrives(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> TorsiondriveRecord: ...

    @overload
    def get_torsiondrives(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> TorsiondriveRecord | None: ...

    @overload
    def get_torsiondrives(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[TorsiondriveRecord]: ...

    @overload
    def get_torsiondrives(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[TorsiondriveRecord | None]: ...

    def get_torsiondrives(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> TorsiondriveRecord | list[TorsiondriveRecord] | list[TorsiondriveRecord | None] | None:
        """
        Obtain torsiondrive records with the specified IDs.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", TorsiondriveRecord, record_ids, missing_ok, include)

    def query_torsiondrives(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        child_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        program: str | Iterable[str] | None = None,
        optimization_program: str | Iterable[str] | None = None,
        qc_program: str | Iterable[str] | None = None,
        qc_method: str | Iterable[str] | None = None,
        qc_basis: str | Iterable[str] | None = None,
        initial_molecule_id: int | Iterable[int] | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[TorsiondriveRecord]:
        """
        Queries torsiondrive records on the server

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose torsiondrive program is in the given list
        optimization_program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        initial_molecule_id
            Query records whose initial molecule (id) is in the given list
        creator_user
            Query records created by a user in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "optimization_program": make_list(optimization_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = TorsiondriveQueryFilters(**filter_dict)
        return RecordQueryIterator[TorsiondriveRecord](self, filter_data, TorsiondriveRecord, include)

    ##############################################################
    # Grid optimization calculations
    ##############################################################

    def add_gridoptimizations(
        self,
        initial_molecules: int | Molecule | Sequence[int | Molecule],
        program: str,
        optimization_specification: OptimizationSpecification,
        keywords: GridoptimizationKeywords | dict[str, Any],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        find_existing: bool = True,
        **kwargs: Any,  # For deprecated parameters
    ) -> tuple[InsertMetadata, list[int]]:
        """
        Adds new gridoptimization computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per initial molecule

        Parameters
        ----------
        initial_molecules
            Molecules to start the gridoptimizations. Each gridoptimization starts with
             a single molecule.
        program
            The program to run the gridoptimization computation with ("gridoptimization")
        optimization_specification
            Specification of how each optimization of the gridoptimization should be run
        keywords
            The gridoptimization keywords for the computation
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data: dict[str, Any] = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "find_existing": find_existing,
        }

        body = GridoptimizationAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/gridoptimization/bulkCreate", tuple[InsertMetadata, list[int]], body=body
        )

    @overload
    def get_gridoptimizations(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> GridoptimizationRecord: ...

    @overload
    def get_gridoptimizations(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> GridoptimizationRecord | None: ...

    @overload
    def get_gridoptimizations(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[GridoptimizationRecord]: ...

    @overload
    def get_gridoptimizations(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[GridoptimizationRecord | None]: ...

    def get_gridoptimizations(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> GridoptimizationRecord | list[GridoptimizationRecord] | list[GridoptimizationRecord | None] | None:
        """
        Obtain gridoptimization records with the specified IDs.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", GridoptimizationRecord, record_ids, missing_ok, include)

    def query_gridoptimizations(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        child_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        program: str | Iterable[str] | None = None,
        optimization_program: str | Iterable[str] | None = None,
        qc_program: str | Iterable[str] | None = None,
        qc_method: str | Iterable[str] | None = None,
        qc_basis: str | Iterable[str | None] | None = None,
        initial_molecule_id: int | Iterable[int] | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[GridoptimizationRecord]:
        """
        Queries gridoptimization records on the server

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose gridoptimization program is in the given list
        optimization_program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        initial_molecule_id
            Query records whose initial molecule (id) is in the given list
        creator_user
            Query records created by a user in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "optimization_program": make_list(optimization_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = GridoptimizationQueryFilters(**filter_dict)
        return RecordQueryIterator[GridoptimizationRecord](self, filter_data, GridoptimizationRecord, include)

    ##############################################################
    # Reactions
    ##############################################################

    def add_reactions(
        self,
        stoichiometries: Sequence[Sequence[tuple[float, int | Molecule]]],
        program: str,
        singlepoint_specification: QCSpecification | None,
        optimization_specification: OptimizationSpecification | None,
        keywords: ReactionKeywords,
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        find_existing: bool = True,
        **kwargs: Any,  # For deprecated parameters
    ) -> tuple[InsertMetadata, list[int]]:
        """
        Adds new reaction computations to the server

        Reactions can have a singlepoint specification, optimization specification,
        or both; at least one must be specified. If both are specified, an optimization
        is done, followed by a singlepoint computation. Otherwise, only the specification
        that is specified is used.

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per reaction

        Parameters
        ----------
        stoichiometries
            Coefficients and molecules of the reaction. Each reaction has multiple
            molecules/coefficients, so this is a nested list
        program
            The program for running the reaction computation ("reaction")
        singlepoint_specification
            The specification for singlepoint energy calculations
        optimization_specification
            The specification for optimization calculations
        keywords
            The keywords for the reaction calculation/service
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input stoichiometries
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        if not stoichiometries:
            return InsertMetadata(), []

        if len(stoichiometries) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(stoichiometries)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data: dict[str, Any] = {
            "stoichiometries": stoichiometries,
            "specification": {
                "program": program,
                "singlepoint_specification": singlepoint_specification,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "find_existing": find_existing,
        }

        body = ReactionAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/reaction/bulkCreate", tuple[InsertMetadata, list[int]], body=body
        )

    @overload
    def get_reactions(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> ReactionRecord: ...

    @overload
    def get_reactions(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> ReactionRecord | None: ...

    @overload
    def get_reactions(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[ReactionRecord]: ...

    @overload
    def get_reactions(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[ReactionRecord | None]: ...

    def get_reactions(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> ReactionRecord | list[ReactionRecord] | list[ReactionRecord | None] | None:
        """
        Obtain reaction records with the specified IDs.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", ReactionRecord, record_ids, missing_ok, include)

    def query_reactions(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        child_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        program: str | Iterable[str] | None = None,
        optimization_program: Iterable[str | None] | None = None,
        qc_program: str | Iterable[str] | None = None,
        qc_method: str | Iterable[str] | None = None,
        qc_basis: str | Iterable[str] | None = None,
        molecule_id: int | Iterable[int] | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[ReactionRecord]:
        """
        Queries reaction records on the server

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (singlepoint or optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose reaction program is in the given list
        optimization_program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        molecule_id
            Query reactions that contain a molecule (id) is in the given list
        creator_user
            Query records created by a user in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "optimization_program": make_list(optimization_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = ReactionQueryFilters(**filter_dict)
        return RecordQueryIterator[ReactionRecord](self, filter_data, ReactionRecord, include)

    ##############################################################
    # Manybody calculations
    ##############################################################

    def add_manybodys(
        self,
        initial_molecules: Sequence[int | Molecule],
        program: str,
        levels: dict[int | Literal["supersystem"], QCSpecification],
        bsse_correction: BSSECorrectionEnum | Sequence[BSSECorrectionEnum],
        keywords: ManybodyKeywords | dict[str, Any],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        find_existing: bool = True,
        **kwargs: Any,  # For deprecated parameters
    ) -> tuple[InsertMetadata, list[int]]:
        """
        Adds new manybody expansion computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per initial molecule.

        Parameters
        ----------
        initial_molecules
            Initial molecules for the manybody expansion. Must have > 1 fragments.
        program
            The program to run the manybody computation with ("manybody")
        levels
            Specification for the singlepoint calculations done in the expansion, keyed by the
            number of bodies they apply to (or "supersystem")
        bsse_correction
            The basis set superposition error correction(s) to compute
        keywords
            The keywords for the manybody program
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data: dict[str, Any] = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "levels": levels,
                "bsse_correction": make_list(bsse_correction),
                "keywords": keywords,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "find_existing": find_existing,
        }

        body = ManybodyAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/manybody/bulkCreate", tuple[InsertMetadata, list[int]], body=body
        )

    @overload
    def get_manybodys(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> ManybodyRecord: ...

    @overload
    def get_manybodys(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> ManybodyRecord | None: ...

    @overload
    def get_manybodys(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[ManybodyRecord]: ...

    @overload
    def get_manybodys(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[ManybodyRecord | None]: ...

    def get_manybodys(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> ManybodyRecord | list[ManybodyRecord] | list[ManybodyRecord | None] | None:
        """
        Obtain manybody records with the specified IDs.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", ManybodyRecord, record_ids, missing_ok, include)

    def query_manybodys(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        child_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        program: str | Iterable[str] | None = None,
        qc_program: str | Iterable[str] | None = None,
        qc_method: str | Iterable[str] | None = None,
        qc_basis: str | Iterable[str] | None = None,
        initial_molecule_id: int | Iterable[int] | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[ManybodyRecord]:
        """
        Queries manybody records on the server

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (singlepoint calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose manybody program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose qc method is in the given list
        qc_basis
            Query records whose qc basis is in the given list
        initial_molecule_id
            Query manybody calculations that contain an initial molecule (id) is in the given list
        creator_user
            Query records created by a user in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = ManybodyQueryFilters(**filter_dict)
        return RecordQueryIterator[ManybodyRecord](self, filter_data, ManybodyRecord, include)

    ##############################################################
    # NEB
    ##############################################################

    def add_nebs(
        self,
        initial_chains: Sequence[Sequence[int | Molecule]],
        program: str,
        singlepoint_specification: QCSpecification,
        optimization_specification: OptimizationSpecification | None,
        keywords: NEBKeywords | dict[str, Any],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        find_existing: bool = True,
        **kwargs: Any,  # For deprecated parameters
    ) -> tuple[InsertMetadata, list[int]]:
        """
        Adds neb calculations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per initial chain

        Parameters
        ----------
        initial_chains
            The initial chains to run the NEB calculations on. Each NEB calculation starts with a single
            chain (list of molecules), so this is a nested list
        program
            The program to run the neb computation with ("geometric")
        singlepoint_specification
            Specification of how each singlepoint (gradient/hessian) should be run
        optimization_specification
            Specification of how the transition state optimization should be run, if one was requested
            with the optimize_ts keyword. May be None. Note that this does not apply to the endpoint
            optimizations requested with the optimize_endpoints keyword, which always use geometric
            with the level of theory given in the singlepoint specification
        keywords
            The NEB keywords for the computation
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input chains
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        if not initial_chains:
            return InsertMetadata(), []

        if len(initial_chains) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_chains)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data: dict[str, Any] = {
            "initial_chains": initial_chains,
            "specification": {
                "program": program,
                "singlepoint_specification": singlepoint_specification,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "find_existing": find_existing,
        }

        body = NEBAddBody(**body_data)

        return self.make_request(
            "post",
            "api/v1/records/neb/bulkCreate",
            tuple[InsertMetadata, list[int]],
            body=body,
        )

    @overload
    def get_nebs(
        self, record_ids: int, missing_ok: Literal[False] = False, *, include: Iterable[str] | None = None
    ) -> NEBRecord: ...

    @overload
    def get_nebs(
        self, record_ids: int, missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> NEBRecord | None: ...

    @overload
    def get_nebs(
        self,
        record_ids: Collection[int],
        missing_ok: Literal[False] = False,
        *,
        include: Iterable[str] | None = None,
    ) -> list[NEBRecord]: ...

    @overload
    def get_nebs(
        self, record_ids: Collection[int], missing_ok: bool, *, include: Iterable[str] | None = None
    ) -> list[NEBRecord | None]: ...

    def get_nebs(
        self,
        record_ids: int | Collection[int],
        missing_ok: bool = False,
        *,
        include: Iterable[str] | None = None,
    ) -> NEBRecord | list[NEBRecord] | list[NEBRecord | None] | None:
        """
        Obtain NEB records with the specified IDs.

        Records will be returned in the same order as the record ids. If the ids are given as an
        unordered collection (a set, for example), that order is however the collection itself
        iterates - it is up to the caller to keep track of which record is which in that case.

        Parameters
        ----------
        record_ids
            A single ID, or a collection of IDs (list, tuple, set, numpy array, ...) to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type("api/v1", NEBRecord, record_ids, missing_ok, include)

    def query_nebs(
        self,
        *,
        record_id: int | Iterable[int] | None = None,
        manager_name: str | Iterable[str] | None = None,
        history_manager_name: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        dataset_id: int | Iterable[int] | None = None,
        project_id: int | Iterable[int] | None = None,
        parent_id: int | Iterable[int] | None = None,
        child_id: int | Iterable[int] | None = None,
        created_before: datetime | str | None = None,
        created_after: datetime | str | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        program: str | Iterable[str] | None = None,
        qc_program: str | Iterable[str] | None = None,
        qc_method: str | Iterable[str] | None = None,
        qc_basis: str | Iterable[str] | None = None,
        molecule_id: int | Iterable[int] | None = None,
        creator_user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
        include: Iterable[str] | None = None,
    ) -> RecordQueryIterator[NEBRecord]:
        """
        Queries neb records from the server

        Do not rely on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        history_manager_name
            Query any records that have been run by the given manager(s), even if not currently assigned
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        project_id
            Query records belonging to the given project IDs
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (singlepoint or optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose neb program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        molecule_id
            Query records whose initial chains contain a molecule (id) that is in the given list
        creator_user
            Query records created by a user in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "history_manager_name": make_list(history_manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "project_id": make_list(project_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "creator_user": make_list(creator_user),
            "limit": limit,
        }

        filter_data = NEBQueryFilters(**filter_dict)
        return RecordQueryIterator[NEBRecord](self, filter_data, NEBRecord, include)

    ##############################################################
    # Managers
    ##############################################################

    # A str is itself a collection of str, so these overloads necessarily overlap with the ones below.
    # Overloads are matched in order, so a single name (a str) is handled by these first
    @overload
    def get_managers(  # type: ignore[overload-overlap]
        self, names: str, missing_ok: Literal[False] = False
    ) -> ComputeManager: ...

    @overload
    def get_managers(self, names: str, missing_ok: bool) -> ComputeManager | None: ...  # type: ignore[overload-overlap]

    @overload
    def get_managers(self, names: Collection[str], missing_ok: Literal[False] = False) -> list[ComputeManager]: ...

    @overload
    def get_managers(self, names: Collection[str], missing_ok: bool) -> list[ComputeManager | None]: ...

    def get_managers(
        self,
        names: str | Collection[str],
        missing_ok: bool = False,
    ) -> ComputeManager | list[ComputeManager] | list[ComputeManager | None] | None:
        """Obtain manager information from the server with the specified names

        Parameters
        ----------
        names
            A single manager name, or a collection of names (list, tuple, set, ...)
        missing_ok
            If set to True, then missing managers will be tolerated, and the returned
            managers will contain None for the corresponding managers that were not found.

        Returns
        -------
        :
            If a single name was specified, returns just that manager. Otherwise, returns a list of
            managers, in the same order as the names given. If missing_ok was specified, None will be
            substituted for a manager that was not found. Note that if the names are given as an
            unordered collection (a set, for example), the order of the results is however that
            collection itself iterates.
        """

        is_single = is_scalar(names)

        name_lst = make_list(names)
        if not name_lst:
            return []

        body = CommonBulkGetNamesBody(names=name_lst, missing_ok=missing_ok)

        managers = self.make_request("post", "api/v1/managers/bulkGet", list[ComputeManager | None], body=body)

        for m in managers:
            if m is not None:
                m.propagate_client(self, "api/v1")

        if is_single:
            return managers[0]
        else:
            return managers

    def query_managers(
        self,
        *,
        manager_id: int | Iterable[int] | None = None,
        name: str | Iterable[str] | None = None,
        cluster: str | Iterable[str] | None = None,
        hostname: str | Iterable[str] | None = None,
        status: RecordStatusEnum | Iterable[RecordStatusEnum] | None = None,
        modified_before: datetime | str | None = None,
        modified_after: datetime | str | None = None,
        limit: int | None = None,
    ) -> ManagerQueryIterator:
        """
        Queries manager information on the server

        Parameters
        ----------
        manager_id
            ID assigned to the manager (this is not the UUID. This should be used very rarely).
        name
            Queries managers whose name is in the given list
        cluster
            Queries managers whose assigned cluster is in the given list
        hostname
            Queries managers whose hostname is in the given list
        status
            Queries managers whose status is in the given list
        modified_before
            Query for managers last modified before a certain time
        modified_after
            Query for managers last modified after a certain time
        limit
            The maximum number of managers to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "manager_id": make_list(manager_id),
            "name": make_list(name),
            "cluster": make_list(cluster),
            "hostname": make_list(hostname),
            "status": make_list(status),
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        filter_data = ManagerQueryFilters(**filter_dict)
        return ManagerQueryIterator(self, filter_data)

    def query_active_managers(
        self,
        compute_tag: str | list[str],
        programs: dict[str, list[str]],
    ) -> list[str]:
        """
        Queries active managers for any that can take the given compute tag or program

        This will select managers that can handle any of the given compute tags, and all the given programs/versions.

        Parameters
        ----------
        compute_tag
            List of possible tags the task can have. A manager must have one of the tags to be considered available.
        programs
            Required programs/versions for the task. A manager must have all the programs/versions to be considered available.

        Returns
        -------
        :
            List of manager names that could possible handle the given compute tags/programs.
        """

        query_data = ManagerQueryAvailableFilters(compute_tag=make_list(compute_tag), programs=programs)
        return self.make_request("post", "api/v1/managers/queryActive", list[str], body=query_data)

    def query_access_log(
        self,
        *,
        module: str | Iterable[str] | None = None,
        method: str | Iterable[str] | None = None,
        before: datetime | str | None = None,
        after: datetime | str | None = None,
        user: int | str | Iterable[int | str] | None = None,
        limit: int | None = None,
    ) -> AccessLogQueryIterator:
        """
        Query the server access log

        This log contains information about who accessed the server, and when.

        Parameters
        ----------
        module
            Return log entries whose module is in the given list
        method
            Return log entries whose access_method is in the given list
        before
            Return log entries captured before the specified date/time
        after
            Return log entries captured after the specified date/time
        user
            User name or ID associated with the log entry
        limit
            The maximum number of log entries to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_data = AccessLogQueryFilters(
            module=make_list(module),
            method=make_list(method),
            before=before,
            after=after,
            user=make_list(user),
            limit=limit,
        )

        return AccessLogQueryIterator(self, filter_data)

    def delete_access_log(self, before: datetime) -> int:
        """
        Delete access log entries from the server

        Parameters
        ----------
        before
            Delete access log entries captured before the given date/time

        Returns
        -------
        :
            The number of access log entries deleted from the server
        """

        body = DeleteBeforeDateBody(before=before)
        return self.make_request("post", "api/v1/access_logs/bulkDelete", int, body=body)

    def query_error_log(
        self,
        *,
        error_id: int | Iterable[int] | None = None,
        user: int | str | Iterable[int | str] | None = None,
        before: datetime | str | None = None,
        after: datetime | str | None = None,
        limit: int | None = None,
    ) -> ErrorLogQueryIterator:
        """
        Query the server's internal error log

        This log contains internal errors that are not always passed to the user.

        Parameters
        ----------
        error_id
            Return error log entries whose id is in the list
        user
            Return error log entries whose user name or ID is in the list
        before
            Return error log entries captured before the specified date/time
        after
            Return error log entries captured after the specified date/time
        limit
            The maximum number of log entries to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_data = ErrorLogQueryFilters(
            error_id=make_list(error_id),
            user=make_list(user),
            before=before,
            after=after,
            limit=limit,
        )

        return ErrorLogQueryIterator(self, filter_data)

    def delete_error_log(self, before: datetime) -> int:
        """
        Delete error log entries from the server

        Parameters
        ----------
        before
            Delete error log entries captured before the given date/time

        Returns
        -------
        :
            The number of error log entries deleted from the server
        """
        body = DeleteBeforeDateBody(before=before)
        return self.make_request("post", "api/v1/server_errors/bulkDelete", int, body=body)

    def get_internal_job(self, job_id: int) -> InternalJob:
        """
        Gets information about an internal job on the server

        Parameters
        ----------
        job_id
            ID of the internal job to obtain

        Returns
        -------
        :
            Information about the internal job, which can be used to watch its progress
        """

        ij_dict = self.make_request("get", f"api/v1/internal_jobs/{job_id}", dict[str, Any])
        return InternalJob(client=self, refresh_url=f"api/v1/internal_jobs/{job_id}", **ij_dict)

    def query_internal_jobs(
        self,
        *,
        job_id: int | Iterable[int] | None = None,
        name: str | Iterable[str] | None = None,
        user: int | str | Iterable[int | str] | None = None,
        runner_hostname: str | Iterable[str] | None = None,
        status: InternalJobStatusEnum | Iterable[InternalJobStatusEnum] | None = None,
        last_updated_before: datetime | str | None = None,
        last_updated_after: datetime | str | None = None,
        added_before: datetime | str | None = None,
        added_after: datetime | str | None = None,
        scheduled_before: datetime | str | None = None,
        scheduled_after: datetime | str | None = None,
        limit: int | None = None,
    ) -> InternalJobQueryIterator:
        """
        Queries the internal job queue on the server

        Parameters
        ----------
        job_id
            ID assigned to the job
        name
            Queries jobs whose name is in the given list
        user
            User name or ID associated with the log entry
        runner_hostname
            Queries jobs that were run/are running on a given host
        status
            Queries jobs whose status is in the given list
        last_updated_before
            Query for jobs last updated before a certain time
        last_updated_after
            Query for jobs last updated after a certain time
        added_before
            Query for jobs added before a certain time
        added_after
            Query for jobs added after a certain time
        scheduled_before
            Query for jobs scheduled to run before a certain time
        scheduled_after
            Query for jobs scheduled to run after a certain time
        limit
            The maximum number of jobs to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict: dict[str, Any] = {
            "job_id": make_list(job_id),
            "name": make_list(name),
            "runner_hostname": make_list(runner_hostname),
            "status": make_list(status),
            "user": make_list(user),
            "last_updated_before": last_updated_before,
            "last_updated_after": last_updated_after,
            "added_before": added_before,
            "added_after": added_after,
            "scheduled_before": scheduled_before,
            "scheduled_after": scheduled_after,
            "limit": limit,
        }

        filter_data = InternalJobQueryFilters(**filter_dict)
        return InternalJobQueryIterator(self, filter_data)

    def cancel_internal_job(self, job_id: int) -> None:
        """
        Cancels (to the best of our ability) an internal job

        A job that is already running may not stop immediately, and one that has already finished
        is not affected.

        Parameters
        ----------
        job_id
            ID of the internal job to cancel
        """

        return self.make_request(
            "put", f"api/v1/internal_jobs/{job_id}/status", None, body=InternalJobStatusEnum.cancelled
        )

    def delete_internal_job(self, job_id: int) -> None:
        """
        Removes an internal job from the server

        Parameters
        ----------
        job_id
            ID of the internal job to delete
        """

        return self.make_request("delete", f"api/v1/internal_jobs/{job_id}", None)

    def query_access_summary(
        self,
        *,
        group_by: str = "day",
        before: datetime | str | None = None,
        after: datetime | str | None = None,
    ) -> AccessLogSummary:
        """Obtains summaries of access data

        This aggregate data is created on the server, so you don't need to download all the
        log entries and do it yourself.

        Parameters
        ----------
        group_by
            How to group the data. Valid options are "user", "hour", "day", "country", "subdivision"
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time

        Returns
        -------
        :
            Aggregated access data, grouped as requested
        """

        url_params = AccessLogSummaryFilters(group_by=group_by, before=before, after=after)

        entries = self.make_request(
            "get", "api/v1/access_logs/summary", dict[str, list[AccessLogSummaryEntry]], url_params=url_params
        )

        return AccessLogSummary(entries=entries)

    ##############################################################
    # User & group management
    ##############################################################

    def list_groups(self) -> list[GroupInfo]:
        """
        List all user groups on the server

        Returns
        -------
        :
            Information about all the groups on the server
        """

        return self.make_request("get", "api/v1/groups", list[GroupInfo])

    def get_group(self, groupname_or_id: int | str) -> GroupInfo:
        """
        Get information about a group on the server

        Parameters
        ----------
        groupname_or_id
            The name or ID of the group to obtain

        Returns
        -------
        :
            Information about the group
        """

        if isinstance(groupname_or_id, str):
            is_valid_groupname(groupname_or_id)

        return self.make_request("get", f"api/v1/groups/{groupname_or_id}", GroupInfo)

    def add_group(self, group_info: GroupInfo) -> None:
        """
        Adds a group with permissions to the server

        If not successful, an exception is raised.

        Parameters
        ----------
        group_info
            Info about the group to add. Must not contain an id
        """

        if group_info.id is not None:
            raise RuntimeError("Cannot add group when group_info contains an id")

        return self.make_request("post", "api/v1/groups", None, body=group_info)

    def delete_group(self, groupname_or_id: int | str) -> None:
        """
        Deletes a group on the server

        Deleted groups will be removed from all users groups list

        Parameters
        ----------
        groupname_or_id
            The name or ID of the group to delete
        """

        if isinstance(groupname_or_id, str):
            is_valid_groupname(groupname_or_id)

        return self.make_request("delete", f"api/v1/groups/{groupname_or_id}", None)

    def list_users(self) -> list[UserInfo]:
        """
        List all users on the server

        Returns
        -------
        :
            Information about all the users on the server
        """

        return self.make_request("get", "api/v1/users", list[UserInfo])

    def get_user(self, username_or_id: int | str | None = None) -> UserInfo:
        """
        Get information about a user on the server

        If the username is not supplied, then info about the currently logged-in user is obtained.

        Parameters
        ----------
        username_or_id
            The username or ID to get info about

        Returns
        -------
        :
            Information about the user
        """

        if username_or_id is None:
            if self.username is None:
                raise RuntimeError("Cannot get user - not logged in?")
            username_or_id = self.username

        if isinstance(username_or_id, str):
            is_valid_username(username_or_id)

        if isinstance(username_or_id, int):
            is_me = username_or_id == self.user_id
        else:
            is_me = username_or_id == self.username

        if is_me:
            return self.make_request("get", f"api/v1/me", UserInfo)
        else:
            return self.make_request("get", f"api/v1/users/{username_or_id}", UserInfo)

    def add_user(self, user_info: UserInfo, password: str | None = None) -> str:
        """
        Adds a user to the server

        Parameters
        ----------
        user_info
            Info about the user to add
        password
            The user's password. If None, then one will be generated

        Returns
        -------
        :
            The password of the user (either the same as the supplied password, or the
            server-generated one)

        """

        if password is not None:
            is_valid_password(password)

        if user_info.id is not None:
            raise RuntimeError("Cannot add user when user_info contains an id")

        return self.make_request("post", "api/v1/users", str, body=(user_info, password))

    def modify_user(self, user_info: UserInfo) -> UserInfo:
        """
        Modifies a user on the server

        The user is determined by the id field of the input UserInfo, although the id
        and username are checked for consistency.

        Depending on the current user's permissions, some fields may not be updatable.

        Parameters
        ----------
        user_info
            Updated information for a user

        Returns
        -------
        :
            The updated user information as it appears on the server
        """

        is_me = user_info.id == self.user_id and user_info.username == self.username

        if is_me:
            return self.make_request("patch", f"api/v1/me", UserInfo, body=user_info)
        else:
            return self.make_request("patch", f"api/v1/users", UserInfo, body=user_info)

    def change_user_password(self, username_or_id: int | str | None = None, new_password: str | None = None) -> str:
        """
        Change a users password

        If the username is not specified, then the current logged-in user is used.

        If the password is not specified, then one is automatically generated by the server.

        Parameters
        ----------
        username_or_id
            The name or ID of the user whose password to change. If None, then use the currently logged-in user
        new_password
            Password to change to. If None, let the server generate one.

        Returns
        -------
        :
            The new password (either the same as the supplied one, or the server generated one
        """

        if username_or_id is None:
            if self.username is None:
                raise RuntimeError("Cannot get user - not logged in?")
            username_or_id = self.username

        if isinstance(username_or_id, str):
            is_valid_username(username_or_id)

        if new_password is not None:
            is_valid_password(new_password)

        if isinstance(username_or_id, int):
            is_me = username_or_id == self.user_id
        else:
            is_me = username_or_id == self.username

        if is_me:
            return self.make_request("put", f"api/v1/me/password", str, body_model=str | None, body=new_password)
        else:
            return self.make_request(
                "put", f"api/v1/users/{username_or_id}/password", str, body_model=str | None, body=new_password
            )

    def delete_user(self, username_or_id: int | str) -> None:
        """
        Delete a user from the server

        Parameters
        ----------
        username_or_id
            The username or ID of the user to delete
        """

        if not isinstance(username_or_id, int):
            is_valid_username(username_or_id)

        return self.make_request("delete", f"api/v1/users/{username_or_id}", None)

    ##############################################################
    # API tokens
    ##############################################################
    def list_api_tokens(self, username_or_id: int | str | None = None) -> list[APIToken]:
        """
        List a user's API tokens (never including the tokens themselves)

        Parameters
        ----------
        username_or_id
            The user whose tokens to list. If None, lists the current user's own tokens.
        """

        if username_or_id is None:
            return self.make_request("get", "api/v1/me/tokens", list[APIToken])

        if not isinstance(username_or_id, int):
            is_valid_username(username_or_id)
        return self.make_request("get", f"api/v1/users/{username_or_id}/tokens", list[APIToken])

    def create_api_token(
        self,
        name: str,
        expires_at: datetime | None = None,
        username_or_id: int | str | None = None,
        scope: str = APITokenScopeEnum.unlimited.value,
    ) -> NewAPIToken:
        """
        Create a new API token

        The returned object contains the plaintext token, which is shown only once and cannot be
        retrieved later.

        Parameters
        ----------
        name
            A name to identify the token. Must be unique among the user's tokens.
        expires_at
            When the token should expire (timezone-aware). If None, the server's default policy
            applies (which may be no expiration).
        username_or_id
            The user to create the token for. If None, creates a token for the current user.
        scope
            What the token should be allowed to do. Currently only "unlimited" (the owner's
            full role) exists; this is a placeholder for future restricted scopes.
        """

        body = APITokenCreateBody(name=name, expires_at=expires_at, scope=scope)

        if username_or_id is None:
            return self.make_request(
                "post", "api/v1/me/tokens", NewAPIToken, body_model=APITokenCreateBody, body=body
            )

        if not isinstance(username_or_id, int):
            is_valid_username(username_or_id)
        return self.make_request(
            "post", f"api/v1/users/{username_or_id}/tokens", NewAPIToken, body_model=APITokenCreateBody, body=body
        )

    def delete_api_token(self, token_id: int, username_or_id: int | str | None = None) -> None:
        """
        Delete (revoke) an API token

        Parameters
        ----------
        token_id
            The id of the token to delete
        username_or_id
            The owner of the token. If None, deletes one of the current user's own tokens.
        """

        if username_or_id is None:
            return self.make_request("delete", f"api/v1/me/tokens/{token_id}", None)

        if not isinstance(username_or_id, int):
            is_valid_username(username_or_id)
        return self.make_request("delete", f"api/v1/users/{username_or_id}/tokens/{token_id}", None)
