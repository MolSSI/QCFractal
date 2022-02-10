from __future__ import annotations

import os
import warnings
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    Sequence,
    Iterable,
)

import pydantic

from .base_models import (
    CommonGetURLParametersName,
    CommonGetProjURLParameters,
    CommonBulkGetBody,
)
from .cache import PortalCache
from .client_base import PortalClientBase
from .datasets import AllDatasetTypes, AllDatasetDataModelTypes, DatasetQueryModel
from .datasets.optimization import OptimizationDatasetAddBody
from .keywords import KeywordSet
from .managers import ManagerQueryBody, ComputeManager
from .metadata_models import QueryMetadata, UpdateMetadata, InsertMetadata, DeleteMetadata
from .molecules import Molecule, MoleculeIdentifiers, MoleculeQueryBody, MoleculeModifyBody
from .permissions import (
    UserInfo,
    RoleInfo,
    is_valid_username,
    is_valid_password,
    is_valid_rolename,
)
from .records import (
    RecordStatusEnum,
    PriorityEnum,
    RecordQueryBody,
    RecordModifyBody,
    RecordDeleteURLParameters,
    RecordRevertBodyParameters,
    AllRecordTypes,
    AllRecordDataModelTypes,
)
from .records.gridoptimization import (
    GridoptimizationKeywords,
    GridoptimizationAddBody,
    GridoptimizationRecord,
)
from .records.optimization import (
    OptimizationProtocols,
    OptimizationRecord,
    OptimizationQueryBody,
    OptimizationQCInputSpecification,
    OptimizationInputSpecification,
    OptimizationAddBody,
)
from .records.singlepoint import (
    SinglepointRecord,
    SinglepointAddBody,
    SinglepointQueryBody,
    SinglepointDriver,
    SinglepointProtocols,
)
from .records.torsiondrive import (
    TorsiondriveKeywords,
    TorsiondriveAddBody,
    TorsiondriveRecord,
    TorsiondriveQueryBody,
)
from .serverinfo import (
    AccessLogQueryParameters,
    AccessLogQuerySummaryParameters,
    ErrorLogQueryParameters,
    ServerStatsQueryParameters,
    DeleteBeforeDateParameters,
)
from .utils import make_list, make_str


# TODO : built-in query limit chunking, progress bars, fs caching and invalidation
class PortalClient(PortalClientBase):
    def __init__(
        self,
        address: str = "api.qcarchive.molssi.org:443",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
        cache: Optional[Union[str, Path]] = None,
        max_memcache_size: Optional[int] = 1000000,
    ) -> None:
        """Initializes a PortalClient instance from an address and verification information.

        Parameters
        ----------
        address
            The IP and port of the FractalServer instance ("192.168.1.1:8888")
        username
            The username to authenticate with.
        password
            The password to authenticate with.
        verify
            Verifies the SSL connection with a third party server. This may be False if a
            FractalServer was not provided a SSL certificate and defaults back to self-signed
            SSL keys.
        cache
            Path to directory to use for cache.
            If None, only in-memory caching used.
        max_memcache_size
            Number of items to hold in client's memory cache.
            Increase this value to improve performance for repeated calls,
            at the cost of higher memory usage.
        """

        PortalClientBase.__init__(self, address, username, password, verify)
        self._cache = PortalCache(self, cachedir=cache, max_memcache_size=max_memcache_size)

    def __repr__(self) -> str:
        """A short representation of the current PortalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "PortalClient(server_name='{}', address='{}', username='{}', cache='{}')".format(
            self.server_name, self.address, self.username, self.cache
        )
        return ret

    def _repr_html_(self) -> str:

        output = f"""
        <h3>PortalClient</h3>
        <ul>
          <li><b>Server:   &nbsp; </b>{self.server_name}</li>
          <li><b>Address:  &nbsp; </b>{self.address}</li>
          <li><b>Username: &nbsp; </b>{self.username}</li>
          <li><b>Cache: &nbsp; </b>{self.cache}</li>
        </ul>
        """

        # postprocess due to raw spacing above
        return "\n".join([substr.strip() for substr in output.split("\n")])

    def recordmodel_from_datamodel(
        self, data: Sequence[Optional[AllRecordDataModelTypes]]
    ) -> List[Optional[AllRecordTypes]]:
        record_init = [
            {"client": self, "record_type": d.record_type, "raw_data": d} if d is not None else None for d in data
        ]

        return pydantic.parse_obj_as(List[Optional[AllRecordTypes]], record_init)

    def datasetmodel_from_datamodel(self, data: AllDatasetDataModelTypes) -> AllDatasetTypes:
        dataset_init = {"client": self, "dataset_type": data.collection_type, "raw_data": data}
        return pydantic.parse_obj_as(AllDatasetTypes, dataset_init)

    @property
    def cache(self):
        if self._cache.cachedir is not None:
            return os.path.relpath(self._cache.cachedir)
        else:
            return None

    def _get_with_cache(self, func, id, missing_ok, entity_type, include=None):
        str_id = make_str(id)
        ids = make_list(str_id)

        # pass through the cache first
        # remove any ids that were found in cache
        # if `include` filters passed, don't use cache, just query DB, as it's often faster
        # for a few fields
        if include is None:
            cached = self._cache.get(ids, entity_type=entity_type)
        else:
            cached = {}

        for i in cached:
            ids.remove(i)

        # if all ids found in cache, no need to go further
        if len(ids) == 0:
            if isinstance(id, list):
                return [cached[i] for i in str_id]
            else:
                return cached[str_id]

        # molecule getting does *not* support "include"
        if include is None:
            payload = {
                "data": {"id": ids},
            }
        else:
            if "id" not in include:
                include.append("id")

            payload = {
                "meta": {"include": include},
                "data": {"id": ids},
            }

        results, to_cache = func(payload)

        # we only cache if no field filtering was done
        if include is None:
            self._cache.put(to_cache, entity_type=entity_type)

        # combine cached records with queried results
        results.update(cached)

        # check that we have results for all ids asked for
        missing = set(make_list(str_id)) - set(results.keys())

        if missing and not missing_ok:
            raise KeyError(f"No objects found for `id`: {missing}")

        # order the results by input id list
        if isinstance(id, list):
            ordered = [results.get(i, None) for i in str_id]
        else:
            ordered = results.get(str_id, None)

        return ordered

    # TODO - needed?
    def _query_cache(self):
        pass

    def get_server_information(self) -> Dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        return self._auto_request("get", "v1/information", None, None, Dict[str, Any], None, None)

    ##############################################################
    # Datasets
    ##############################################################
    def list_datasets(self, dataset_type: Optional[Union[str, Iterable[str]]]):
        pass

    def get_dataset(self, dataset_type: str, dataset_name: str):

        payload = {
            "dataset_type": dataset_type,
            "dataset_name": dataset_name,
            "include": ["*", "specifications.*", "specifications.specification"],
        }

        ds = self._auto_request(
            "post",
            f"v1/dataset/query",
            None,
            DatasetQueryModel,
            AllDatasetDataModelTypes,
            None,
            payload,
        )

        return self.datasetmodel_from_datamodel(ds)

    def get_dataset_by_id(self, dataset_id: int):

        payload = {"include": ["*", "specifications.*", "specifications.specification"]}

        ds = self._auto_request(
            "get",
            f"v1/dataset/{dataset_id}",
            None,
            CommonGetProjURLParameters,
            AllDatasetDataModelTypes,
            None,
            payload,
        )

        return self.datasetmodel_from_datamodel(ds)

    def add_optimization_dataset(
        self,
        name: str,
        description: Optional[str] = None,
        tagline: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        group: Optional[str] = None,
        provenance: Optional[Dict[str, Any]] = None,
        visibility: bool = True,
        default_tag: str = "*",
        default_priority: PriorityEnum = PriorityEnum.normal,
    ):

        payload = {
            "name": name,
            "description": description,
            "tagline": tagline,
            "tags": tags,
            "group": group,
            "provenance": provenance,
            "visibility": visibility,
            "default_tag": default_tag,
            "default_priority": default_priority,
        }

        ds_id = self._auto_request(
            "post", f"v1/dataset/optimization", OptimizationDatasetAddBody, None, int, payload, None
        )

        return self.get_dataset_by_id(ds_id)

    ##############################################################
    # Molecules
    ##############################################################

    def get_molecules(
        self,
        molecule_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
    ) -> Union[Optional[Molecule], List[Optional[Molecule]]]:
        """Obtains molecules from the server via molecule ids

        Parameters
        ----------
        molecule_id
            An id or list of ids to query.
        missing_ok
            If True, return ``None`` for ids that were not found on the server.
            If False, raise ``KeyError`` if any ids were not found on the server.

        Returns
        -------
        :
            The requested molecules, in the same order as the requested ids.
            If given a list of ids, the return value will be a list.
            Otherwise, it will be a single Molecule.
        """

        molecule_id_lst = make_list(molecule_id)
        if not molecule_id_lst:
            return []

        body_data = CommonBulkGetBody(id=molecule_id_lst, missing_ok=missing_ok)
        mols = self._auto_request(
            "post", "v1/molecules/bulkGet", CommonBulkGetBody, None, List[Optional[Molecule]], body_data, None
        )

        if isinstance(molecule_id, Sequence):
            return mols
        else:
            return mols[0]

    # TODO: we would like more fields to be queryable via the REST API for mols
    #       e.g. symbols/elements. Unless these are indexed might not be performant.
    # TODO: what was paginate: bool = False for?
    def query_molecules(
        self,
        molecule_hash: Optional[Union[str, Iterable[str]]] = None,
        molecular_formula: Optional[Union[str, Iterable[str]]] = None,
        identifiers: Optional[Dict[str, Union[str, Iterable[str]]]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, List[Molecule]]:
        """Query molecules by attributes.

        All matching molecules, up to the lower of `limit` or the server's
        maximum result count, will be returned.

        The return list will be in an indeterminate order

        Parameters
        ----------
        molecule_hash
            Queries molecules by hash
        molecular_formula
            Queries molecules by molecular formula
            Molecular formulas are not order-sensitive (e.g. "H2O == OH2 != Oh2").
        identifiers
            Additional identifiers to search for (smiles, etc)
        limit
            The maximum number of Molecules to query.
        skip
            The number of Molecules to skip in the query, used during pagination
        """

        if limit is not None and limit > self.api_limits["get_molecules"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_molecules"])

        query_body = {
            "molecule_hash": make_list(molecule_hash),
            "molecular_formula": make_list(molecular_formula),
            "limit": limit,
            "skip": skip,
        }

        if identifiers is not None:
            query_body["identifiers"] = {k: make_list(v) for k, v in identifiers.items()}

        meta, molecules = self._auto_request(
            "post",
            "v1/molecules/query",
            MoleculeQueryBody,
            None,
            Tuple[QueryMetadata, List[Molecule]],
            query_body,
            None,
        )
        return meta, molecules

    def add_molecules(self, molecules: Sequence[Molecule]) -> Tuple[InsertMetadata, List[int]]:
        """Add molecules to the server.

        Parameters
        molecules
            A list of Molecules to add to the server.

        Returns
        -------
        :
            A list of Molecule ids in the same order as the `molecules` parameter.
        """

        if not molecules:
            return InsertMetadata(), []

        if len(molecules) > self.api_limits["add_molecules"]:
            raise RuntimeError(
                f"Cannot add {len(molecules)} molecules - over the limit of {self.api_limits['add_molecules']}"
            )

        mols = self._auto_request(
            "post",
            "v1/molecules/bulkCreate",
            List[Molecule],
            None,
            Tuple[InsertMetadata, List[int]],
            make_list(molecules),
            None,
        )
        return mols

    def modify_molecule(
        self,
        molecule_id: int,
        name: Optional[str] = None,
        comment: Optional[str] = None,
        identifiers: Optional[Union[Dict[str, Any], MoleculeIdentifiers]] = None,
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

        body = {
            "name": name,
            "comment": comment,
            "identifiers": identifiers,
            "overwrite_identifiers": overwrite_identifiers,
        }

        return self._auto_request(
            "patch", f"v1/molecules/{molecule_id}", MoleculeModifyBody, None, UpdateMetadata, body, None
        )

    def delete_molecules(self, molecule_id: Union[int, Sequence[int]]) -> DeleteMetadata:
        """Deletes molecules from the server

        This will not delete any keywords that are in use

        Parameters
        ----------
        molecule_id
            An id or list of ids to query.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        molecule_id = make_list(molecule_id)
        if not molecule_id:
            return DeleteMetadata()

        return self._auto_request("post", "v1/molecules/bulkDelete", List[int], None, DeleteMetadata, molecule_id, None)

    ##############################################################
    # Keywords
    ##############################################################

    def get_keywords(
        self,
        keywords_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
    ) -> Union[Optional[KeywordSet], List[Optional[KeywordSet]]]:
        """Obtains keywords from the server via keyword ids

        Parameters
        ----------
        keywords_id
            An id or list of ids to query.
        missing_ok
            If True, return ``None`` for ids that were not found on the server.
            If False, raise ``KeyError`` if any ids were not found on the server.

        Returns
        -------
        :
            The requested keywords, in the same order as the requested ids.
            If given a list of ids, the return value will be a list.
            Otherwise, it will be a single KeywordSet.
        """

        keywords_id_lst = make_list(keywords_id)
        if not keywords_id_lst:
            return []

        body_data = CommonBulkGetBody(id=keywords_id_lst, missing_ok=missing_ok)

        if len(body_data.id) > self.api_limits["get_keywords"]:
            raise RuntimeError(
                f"Cannot get {len(body_data.id)} keywords - over the limit of {self.api_limits['get_keywords']}"
            )

        keywords = self._auto_request(
            "post", "v1/keywords/bulkGet", CommonBulkGetBody, None, List[Optional[KeywordSet]], body_data, None
        )

        if isinstance(keywords_id, Sequence):
            return keywords
        else:
            return keywords[0]

    def add_keywords(self, keywords: Sequence[KeywordSet]) -> Union[List[int], Tuple[InsertMetadata, List[int]]]:
        """Adds keywords to the server

        This function is not expected to be used by end users

        Parameters
        ----------
        keywords
            A KeywordSet or list of KeywordSet to add to the server.

        Returns
        -------
        :
            A list of KeywordSet ids that were added or existing on the server, in the
            same order as specified in the keywords parameter. If full_return is True,
            this function will return a tuple containing metadata and the ids.
        """

        keywords = make_list(keywords)

        if len(keywords) == 0:
            return InsertMetadata(), []

        if len(keywords) > self.api_limits["add_keywords"]:
            raise RuntimeError(
                f"Cannot add {len(keywords)} keywords - over the limit of {self.api_limits['add_keywords']}"
            )

        return self._auto_request(
            "post", "v1/keywords/bulkCreate", List[KeywordSet], None, Tuple[InsertMetadata, List[int]], keywords, None
        )

    def delete_keywords(self, keywords_id: Union[int, Sequence[int]]) -> DeleteMetadata:
        """Deletes keywords from the server

        This will not delete any keywords that are in use

        Parameters
        ----------
        keywords_id
            An id or list of ids to query.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        keywords_id = make_list(keywords_id)
        if not keywords_id:
            return DeleteMetadata()

        return self._auto_request("post", "v1/keywords/bulkDelete", List[int], None, DeleteMetadata, keywords_id, None)

    ##############################################################
    # General record functions
    ##############################################################

    def get_records(
        self,
        record_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include_task: bool = False,
        include_service: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
    ) -> Union[List[Optional[AllRecordTypes]], Optional[AllRecordTypes]]:
        """Get result records by id."""

        record_id_lst = make_list(record_id)
        if not record_id_lst:
            return []

        url_params = {"id": record_id_lst, "missing_ok": missing_ok}

        if len(url_params["id"]) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(url_params['id'])} records - over the limit of {self.api_limits['get_records']}"
            )

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_service:
            include |= {"*", "service.*", "service.dependencies"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}

        if include:
            url_params["include"] = include

        record_data = self._auto_request(
            "get",
            "v1/record",
            None,
            CommonGetProjURLParameters,
            List[Optional[AllRecordDataModelTypes]],
            None,
            url_params,
        )

        records = self.recordmodel_from_datamodel(record_data)

        if isinstance(record_id, Sequence):
            return records
        else:
            return records[0]

    def query_records(
        self,
        record_id: Optional[Iterable[int]] = None,
        record_type: Optional[Iterable[str]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        limit: int = None,
        skip: int = 0,
        *,
        include_task: bool = False,
        include_service: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
    ) -> Tuple[QueryMetadata, List[AllRecordTypes]]:

        if limit is not None and limit > self.api_limits["get_records"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_records"])

        query_data = {
            "record_id": make_list(record_id),
            "record_type": make_list(record_type),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_service:
            include |= {"*", "service"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}

        if include:
            query_data["include"] = include

        meta, record_data = self._auto_request(
            "post",
            "v1/record/query",
            RecordQueryBody,
            None,
            Tuple[QueryMetadata, List[AllRecordDataModelTypes]],
            query_data,
            None,
        )

        return meta, self.recordmodel_from_datamodel(record_data)

    def reset_records(self, record_id: Union[int, Sequence[int]]) -> UpdateMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()

        body_data = {"record_id": record_id, "status": RecordStatusEnum.waiting}
        return self._auto_request("patch", "v1/record", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def cancel_records(self, record_id: Union[int, Sequence[int]]) -> UpdateMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()

        body_data = {"record_id": record_id, "status": RecordStatusEnum.cancelled}
        return self._auto_request("patch", "v1/record", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def invalidate_records(self, record_id: Union[int, Sequence[int]]) -> UpdateMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()

        body_data = {"record_id": record_id, "status": RecordStatusEnum.invalid}
        return self._auto_request("patch", "v1/record", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def delete_records(
        self, record_id: Union[int, Sequence[int]], soft_delete=True, delete_children: bool = True
    ) -> DeleteMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return DeleteMetadata()

        url_params = {"record_id": record_id, "soft_delete": soft_delete, "delete_children": delete_children}
        return self._auto_request(
            "delete", "v1/record", None, RecordDeleteURLParameters, DeleteMetadata, None, url_params
        )

    def uninvalidate_records(self, record_id: Union[int, Sequence[int]]) -> UpdateMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()

        body = {"record_id": record_id, "revert_status": RecordStatusEnum.invalid}
        return self._auto_request(
            "post", "v1/record/revert", RecordRevertBodyParameters, None, UpdateMetadata, body, None
        )

    def uncancel_records(self, record_id: Union[int, Sequence[int]]) -> UpdateMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()

        body = {"record_id": record_id, "revert_status": RecordStatusEnum.cancelled}
        return self._auto_request(
            "post", "v1/record/revert", RecordRevertBodyParameters, None, UpdateMetadata, body, None
        )

    def undelete_records(self, record_id: Union[int, Sequence[int]]) -> UpdateMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()

        body = {"record_id": record_id, "revert_status": RecordStatusEnum.deleted}
        return self._auto_request(
            "post", "v1/record/revert", RecordRevertBodyParameters, None, UpdateMetadata, body, None
        )

    def modify_records(
        self,
        record_id: Union[int, Sequence[int]],
        new_tag: Optional[str] = None,
        new_priority: Optional[PriorityEnum] = None,
    ) -> UpdateMetadata:
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()
        if new_tag is None and new_priority is None:
            return UpdateMetadata()

        body_data = {
            "record_id": record_id,
            "tag": new_tag,
            "priority": new_priority,
        }
        return self._auto_request("patch", "v1/record", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def add_comment(self, record_id: Union[int, Sequence[int]], comment: str) -> UpdateMetadata:
        """
        Adds a comment to records

        Parameters
        ----------
        record_id
            The record or records to add the comments to

        comment
            The comment string to add. You username will be added automatically

        Returns
        -------
        :
            Metadata about which records were updated
        """
        record_id = make_list(record_id)
        if not record_id:
            return UpdateMetadata()

        body_data = {
            "record_id": record_id,
            "comment": comment,
        }
        return self._auto_request("patch", "v1/record", RecordModifyBody, None, UpdateMetadata, body_data, None)

    ##############################################################
    # Singlepoint calculations
    ##############################################################

    def add_singlepoints(
        self,
        molecules: Union[int, Molecule, List[Union[int, Molecule]]],
        program: str,
        driver: str,
        method: str,
        basis: Optional[str],
        keywords: Optional[Union[KeywordSet, Dict[str, Any], int]] = None,
        protocols: Optional[Union[SinglepointProtocols, Dict[str, Any]]] = None,
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds a "single" compute to the server.

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
            The KeywordSet ObjectId to use with the given compute
        priority
            The priority of the job {"HIGH", "MEDIUM", "LOW"}. Default is "MEDIUM".
        protocols
            Protocols for store more or less data per field
        tag
            The computational tag to add to your compute, managers can optionally only pull
            based off the string tags. These tags are arbitrary, but several examples are to
            use "large", "medium", "small" to denote the size of the job or "project1", "project2"
            to denote different projects.

        Returns
        -------
        :
            A list of record ids (one per molecule) that were added or existing on the server, in the
            same order as specified in the molecules.keywords parameter
        """

        molecules = make_list(molecules)
        if not molecules:
            return InsertMetadata(), []

        body_data = {
            "molecules": molecules,
            "specification": {
                "program": program,
                "driver": driver,
                "method": method,
                "basis": basis,
            },
            "tag": tag,
            "priority": priority,
        }

        if isinstance(keywords, dict):
            # Turn this into a keyword set
            keywords = KeywordSet(values=keywords)

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        if len(body_data["molecules"]) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(body_data['molecules'])} records - over the limit of {self.api_limits['add_records']}"
            )

        return self._auto_request(
            "post", "v1/record/singlepoint", SinglepointAddBody, None, Tuple[InsertMetadata, List[int]], body_data, None
        )

    def get_singlepoints(
        self,
        record_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include_task: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_molecule: bool = False,
        include_wavefunction: bool = False,
    ) -> Union[Optional[SinglepointRecord], List[Optional[SinglepointRecord]]]:

        record_id_lst = make_list(record_id)
        if not record_id_lst:
            return []

        url_params = {"id": record_id_lst, "missing_ok": missing_ok}

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_molecule:
            include |= {"*", "molecule"}
        if include_wavefunction:
            include |= {"*", "wavefunction"}

        if include:
            url_params["include"] = include

        if len(url_params["id"]) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(url_params['id'])} records - over the limit of {self.api_limits['get_records']}"
            )

        record_data = self._auto_request(
            "get",
            "v1/record/singlepoint",
            None,
            CommonGetProjURLParameters,
            List[Optional[SinglepointRecord._DataModel]],
            None,
            url_params,
        )

        records = self.recordmodel_from_datamodel(record_data)

        if isinstance(record_id, Sequence):
            return records
        else:
            return records[0]

    def query_singlepoints(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        driver: Optional[Iterable[SinglepointDriver]] = None,
        method: Optional[Iterable[str]] = None,
        basis: Optional[Iterable[Optional[str]]] = None,
        keywords_id: Optional[Iterable[int]] = None,
        molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        include_task: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_molecule: bool = False,
        include_wavefunction: bool = False,
    ) -> Tuple[QueryMetadata, List[SinglepointRecord]]:
        """Queries SinglepointRecords from the server."""

        if limit is not None and limit > self.api_limits["get_records"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_records"])

        query_data = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "program": make_list(program),
            "driver": make_list(driver),
            "method": make_list(method),
            "basis": make_list(basis),
            "keywords_id": make_list(keywords_id),
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_molecule:
            include |= {"*", "molecule"}
        if include_wavefunction:
            include |= {"*", "wavefuntion"}

        if include:
            query_data["include"] = include

        meta, record_data = self._auto_request(
            "post",
            "v1/record/singlepoint/query",
            SinglepointQueryBody,
            None,
            Tuple[QueryMetadata, List[SinglepointRecord._DataModel]],
            query_data,
            None,
        )

        return meta, self.recordmodel_from_datamodel(record_data)

    ##############################################################
    # Optimization calculations
    ##############################################################

    def add_optimizations(
        self,
        initial_molecules: Union[int, Molecule, List[Union[int, Molecule]]],
        program: str,
        qc_specification: OptimizationQCInputSpecification,
        keywords: Optional[Union[KeywordSet, Dict[str, Any], int]] = None,
        protocols: Optional[OptimizationProtocols] = None,
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds optimization calculations to the server
        """

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        body_data = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "qc_specification": qc_specification,
            },
            "tag": tag,
            "priority": priority,
        }

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        if len(body_data["initial_molecules"]) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot get {len(body_data['initial_molecules'])} records - over the limit of {self.api_limits['add_records']}"
            )

        return self._auto_request(
            "post",
            "v1/record/optimization",
            OptimizationAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_optimizations(
        self,
        record_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include_task: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_initial_molecule: bool = False,
        include_final_molecule: bool = False,
        include_trajectory: bool = False,
    ) -> Union[Optional[OptimizationRecord], List[Optional[OptimizationRecord]]]:

        record_id_lst = make_list(record_id)
        if not record_id_lst:
            return []

        url_params = {"id": record_id_lst, "missing_ok": missing_ok}

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_initial_molecule:
            include |= {"*", "initial_molecule"}
        if include_final_molecule:
            include |= {"*", "final_molecule"}
        if include_trajectory:
            include |= {"*", "trajectory"}

        if include:
            url_params["include"] = include

        if len(url_params["id"]) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(url_params['id'])} records - over the limit of {self.api_limits['get_records']}"
            )

        record_data = self._auto_request(
            "get",
            "v1/record/optimization",
            None,
            CommonGetProjURLParameters,
            List[Optional[OptimizationRecord._DataModel]],
            None,
            url_params,
        )

        records = self.recordmodel_from_datamodel(record_data)

        if isinstance(record_id, Sequence):
            return records
        else:
            return records[0]

    def query_optimizations(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        qc_keywords_id: Optional[Iterable[int]] = None,
        initial_molecule_id: Optional[Iterable[int]] = None,
        final_molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        include_task: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_initial_molecule: bool = False,
        include_final_molecule: bool = False,
        include_trajectory: bool = False,
    ) -> Tuple[QueryMetadata, List[OptimizationRecord]]:
        """Queries OptimizationRecords from the server."""

        if limit is not None and limit > self.api_limits["get_records"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_records"])

        query_data = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "program": make_list(program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "qc_keywords_id": make_list(qc_keywords_id),
            "initial_molecule_id": make_list(initial_molecule_id),
            "final_molecule_id": make_list(final_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_initial_molecule:
            include |= {"*", "initial_molecule"}
        if include_final_molecule:
            include |= {"*", "final_molecule"}
        if include_trajectory:
            include |= {"*", "trajectory"}

        if include:
            query_data["include"] = include

        meta, record_data = self._auto_request(
            "post",
            "v1/record/optimization/query",
            OptimizationQueryBody,
            None,
            Tuple[QueryMetadata, List[OptimizationRecord._DataModel]],
            query_data,
            None,
        )

        return meta, self.recordmodel_from_datamodel(record_data)

    ##############################################################
    # Torsiondrive calculations
    ##############################################################

    def add_torsiondrives(
        self,
        initial_molecules: List[List[Union[int, Molecule]]],
        program: str,
        optimization_specification: OptimizationInputSpecification,
        keywords: Union[TorsiondriveKeywords, Dict[str, Any]],
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds torsiondrive calculations to the server
        """

        if not initial_molecules:
            return InsertMetadata(), []

        body_data = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "as_service": True,
            "tag": tag,
            "priority": priority,
        }

        if len(body_data["initial_molecules"]) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot get {len(body_data['initial_molecules'])} records - over the limit of {self.api_limits['add_records']}"
            )

        return self._auto_request(
            "post",
            "v1/record/torsiondrive",
            TorsiondriveAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_torsiondrives(
        self,
        record_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include_task: bool = False,
        include_service: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_initial_molecules: bool = False,
        include_optimizations: bool = False,
    ) -> Union[Optional[TorsiondriveRecord], List[Optional[TorsiondriveRecord]]]:

        record_id_lst = make_list(record_id)
        if not record_id_lst:
            return []

        url_params = {"id": record_id_lst, "missing_ok": missing_ok}

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_service:
            include |= {"*", "service"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_initial_molecules:
            include |= {"*", "initial_molecules"}
        if include_optimizations:
            include |= {"*", "optimizations"}

        if include:
            url_params["include"] = include

        if len(url_params["id"]) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(url_params['id'])} records - over the limit of {self.api_limits['get_records']}"
            )

        record_data = self._auto_request(
            "get",
            "v1/record/torsiondrive",
            None,
            CommonGetProjURLParameters,
            List[Optional[TorsiondriveRecord._DataModel]],
            None,
            url_params,
        )

        records = self.recordmodel_from_datamodel(record_data)

        if isinstance(record_id, Sequence):
            return records
        else:
            return records[0]

    def query_torsiondrives(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        optimization_program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        qc_keywords_id: Optional[Iterable[int]] = None,
        initial_molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        include_task: bool = False,
        include_service: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_initial_molecules: bool = False,
        include_optimizations: bool = False,
    ) -> Tuple[QueryMetadata, List[TorsiondriveRecord]]:
        """Queries torsiondrive records from the server."""

        if limit is not None and limit > self.api_limits["get_records"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_records"])

        query_data = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "program": make_list(program),
            "optimization_program": make_list(optimization_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "qc_keywords_id": make_list(qc_keywords_id),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_service:
            include |= {"*", "service"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_initial_molecules:
            include |= {"*", "initial_molecules"}
        if include_optimizations:
            include |= {"*", "optimizations"}

        if include:
            query_data["include"] = include

        meta, record_data = self._auto_request(
            "post",
            "v1/record/torsiondrive/query",
            TorsiondriveQueryBody,
            None,
            Tuple[QueryMetadata, List[TorsiondriveRecord._DataModel]],
            query_data,
            None,
        )

        return meta, self.recordmodel_from_datamodel(record_data)

    ##############################################################
    # Grid optimization calculations
    ##############################################################

    def add_gridoptimizations(
        self,
        initial_molecules: Union[int, Molecule, Sequence[Union[int, Molecule]]],
        program: str,
        optimization_specification: OptimizationInputSpecification,
        keywords: Union[GridoptimizationKeywords, Dict[str, Any]],
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds gridoptimization calculations to the server
        """

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        body_data = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "tag": tag,
            "priority": priority,
        }

        if len(body_data["initial_molecules"]) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot get {len(body_data['initial_molecules'])} records - over the limit of {self.api_limits['add_records']}"
            )

        return self._auto_request(
            "post",
            "v1/record/gridoptimization",
            GridoptimizationAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_gridoptimizations(
        self,
        record_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include_service: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_initial_molecule: bool = False,
        include_starting_molecule: bool = False,
        include_optimizations: bool = False,
    ) -> Union[Optional[GridoptimizationRecord], List[Optional[GridoptimizationRecord]]]:

        record_id_lst = make_list(record_id)
        if not record_id_lst:
            return []

        url_params = {"id": record_id_lst, "missing_ok": missing_ok}

        include = set()

        # We must add '*' so that all the default fields are included
        if include_service:
            include |= {"*", "service"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_initial_molecule:
            include |= {"*", "initial_molecule"}
        if include_starting_molecule:
            include |= {"*", "starting_molecule"}
        if include_optimizations:
            include |= {"*", "optimizations"}

        if include:
            url_params["include"] = include

        if len(url_params["id"]) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(url_params['id'])} records - over the limit of {self.api_limits['get_records']}"
            )

        record_data = self._auto_request(
            "get",
            "v1/record/gridoptimization",
            None,
            CommonGetProjURLParameters,
            List[Optional[GridoptimizationRecord._DataModel]],
            None,
            url_params,
        )

        records = self.recordmodel_from_datamodel(record_data)

        if isinstance(record_id, Sequence):
            return records
        else:
            return records[0]

    def query_gridoptimizations(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        optimization_program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        qc_keywords_id: Optional[Iterable[int]] = None,
        initial_molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        include_task: bool = False,
        include_service: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_initial_molecule: bool = False,
        include_optimizations: bool = False,
    ) -> Tuple[QueryMetadata, List[GridoptimizationRecord]]:
        """Queries torsiondrive records from the server."""

        if limit is not None and limit > self.api_limits["get_records"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_records"])

        query_data = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "program": make_list(program),
            "optimization_program": make_list(optimization_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "qc_keywords_id": make_list(qc_keywords_id),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_service:
            include |= {"*", "service"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_initial_molecule:
            include |= {"*", "initial_molecule"}
        if include_optimizations:
            include |= {"*", "optimizations"}

        if include:
            query_data["include"] = include

        meta, record_data = self._auto_request(
            "post",
            "v1/record/gridoptimization/query",
            TorsiondriveQueryBody,
            None,
            Tuple[QueryMetadata, List[GridoptimizationRecord._DataModel]],
            query_data,
            None,
        )

        return meta, self.recordmodel_from_datamodel(record_data)

    ##############################################################
    # Managers
    ##############################################################

    def get_managers(
        self,
        name: Union[str, Sequence[str]],
        missing_ok: bool = False,
    ) -> Union[Optional[ComputeManager], List[Optional[ComputeManager]]]:
        """Obtains manager information from the server via name

        Parameters
        ----------
        name
            A manager name or list of names
        missing_ok
            If True, return ``None`` for managers that were not found on the server.
            If False, raise ``KeyError`` if any managers were not found on the server.

        Returns
        -------
        :
            The requested managers, in the same order as the requested ids.
            If given a list of ids, the return value will be a list.
            Otherwise, it will be a single manager.
        """

        name_lst = make_list(name)
        if not name_lst:
            return []

        url_params = {"name": name_lst, "missing_ok": missing_ok}
        managers = self._auto_request(
            "get", "v1/manager", None, CommonGetURLParametersName, List[Optional[ComputeManager]], None, url_params
        )

        if isinstance(name, Sequence):
            return managers
        else:
            return managers[0]

    def query_managers(
        self,
        id: Optional[Union[int, Iterable[int]]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        cluster: Optional[Union[str, Iterable[str]]] = None,
        hostname: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        include_log: bool = False,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, Dict[str, Any]]:
        """Obtains information about compute managers attached to this Fractal instance

        Parameters
        ----------
        id
            ID assigned to the manager (this is not the UUID. This should be used very rarely).
        name
            Queries the managers name
        cluster
            Queries the managers cluster
        hostname
            Queries the managers hostname
        status
            Queries the manager's status field
        modified_before
            Query for managers last modified before a certain time
        modified_after
            Query for managers last modified after a certain time
        include_log
            If True, include the log entries for the manager
        limit
            The maximum number of managers to query
        skip
            The number of managers to skip in the query, used during pagination

        Returns
        -------
        :
            Metadata about the query results, and a list of dictionaries with information matching the specified query.
        """

        if limit is not None and limit > self.api_limits["get_managers"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_managers"])

        query_body = {
            "id": make_list(id),
            "name": make_list(name),
            "cluster": make_list(cluster),
            "hostname": make_list(hostname),
            "status": make_list(status),
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        if include_log:
            query_body["include"] = ["*", "log"]

        return self._auto_request(
            "post",
            "v1/manager/query",
            ManagerQueryBody,
            None,
            Tuple[QueryMetadata, List[ComputeManager]],
            query_body,
            None,
        )

    ##############################################################
    # Server statistics and logs
    ##############################################################

    def query_server_stats(
        self,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, List[Dict[str, Any]]]:
        """Obtains individual entries in the server stats logs"""

        if limit is not None and limit > self.api_limits["get_server_stats"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_server_stats"])

        url_params = {"before": before, "after": after, "limit": limit, "skip": skip}
        return self._auto_request(
            "get",
            "v1/server_stats",
            None,
            ServerStatsQueryParameters,
            Tuple[QueryMetadata, List[Dict[str, Any]]],
            None,
            url_params,
        )

    def delete_server_stats(self, before: datetime):
        url_params = {"before": before}
        return self._auto_request("delete", "v1/server_stats", None, DeleteBeforeDateParameters, int, None, url_params)

    def query_access_log(
        self,
        access_type: Optional[Union[str, Iterable[str]]] = None,
        access_method: Optional[Union[str, Iterable[str]]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, List[Dict[str, Any]]]:
        """Obtains individual entries in the access logs"""

        if limit is not None and limit > self.api_limits["get_access_logs"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_access_logs"])

        url_params = {
            "access_type": make_list(access_type),
            "access_method": make_list(access_method),
            "before": before,
            "after": after,
            "limit": limit,
            "skip": skip,
        }

        return self._auto_request(
            "get",
            "v1/access",
            None,
            AccessLogQueryParameters,
            Tuple[QueryMetadata, List[Dict[str, Any]]],
            None,
            url_params,
        )

    def delete_access_log(self, before: datetime):
        url_params = {"before": before}
        return self._auto_request("delete", "v1/access", None, DeleteBeforeDateParameters, int, None, url_params)

    def query_error_log(
        self,
        id: Optional[Union[int, Iterable[int]]] = None,
        username: Optional[Union[str, Iterable[str]]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, Dict[str, Any]]:
        """Obtains individual entries in the error logs"""

        if limit is not None and limit > self.api_limits["get_error_logs"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_error_logs"])

        url_params = {
            "id": make_list(id),
            "username": make_list(username),
            "before": before,
            "after": after,
            "limit": limit,
            "skip": skip,
        }

        return self._auto_request(
            "get",
            "v1/server_error",
            None,
            ErrorLogQueryParameters,
            Tuple[QueryMetadata, List[Dict[str, Any]]],
            None,
            url_params,
        )

    def delete_error_log(self, before: datetime):
        url_params = {"before": before}
        return self._auto_request("delete", "v1/server_error", None, DeleteBeforeDateParameters, int, None, url_params)

    def query_access_summary(
        self,
        group_by: str = "day",
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Obtains daily summaries of accesses

        Parameters
        ----------
        group_by
            How to group the data. Valid options are "user", "hour", "day", "country", "subdivision"
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        """

        url_params = {
            "group_by": group_by,
            "before": before,
            "after": after,
        }

        return self._auto_request(
            "get", "v1/access/summary", None, AccessLogQuerySummaryParameters, Dict[str, Any], None, url_params
        )

    ##############################################################
    # User & role management
    ##############################################################

    def list_roles(self) -> List[RoleInfo]:
        """
        List all user roles on the server
        """

        return self._auto_request("get", "v1/role", None, None, List[RoleInfo], None, None)

    def get_role(self, rolename: str) -> RoleInfo:
        """
        Get information about a role on the server
        """

        is_valid_rolename(rolename)
        return self._auto_request("get", f"v1/role/{rolename}", None, None, RoleInfo, None, None)

    def add_role(self, role_info: RoleInfo) -> None:
        """
        Adds a role with permissions to the server

        If not successful, an exception is raised.
        """

        is_valid_rolename(role_info.rolename)
        return self._auto_request("post", "v1/role", RoleInfo, None, None, role_info, None)

    def modify_role(self, role_info: RoleInfo) -> RoleInfo:
        """
        Modifies the permissions of a role on the server

        If not successful, an exception is raised.

        Returns
        -------
        :
            A copy of the role as it now appears on the server
        """

        is_valid_rolename(role_info.rolename)
        return self._auto_request("put", f"v1/role/{role_info.rolename}", RoleInfo, None, RoleInfo, role_info, None)

    def delete_role(self, rolename: str) -> None:
        """
        Deletes a role from the server

        This will not delete any role to which a user is assigned

        Will raise an exception on error

        Parameters
        ----------
        rolename
            Name of the role to delete

        """
        is_valid_rolename(rolename)
        return self._auto_request("delete", f"v1/role/{rolename}", None, None, None, None, None)

    def list_users(self) -> List[UserInfo]:
        """
        List all user roles on the server
        """

        return self._auto_request("get", "v1/user", None, None, List[UserInfo], None, None)

    def get_user(self, username: Optional[str] = None, as_admin: bool = False) -> UserInfo:
        """
        Get information about a user on the server

        If the username is not supplied, then info about the currently logged-in user is obtained

        Parameters
        ----------
        username
            The username to get info about
        as_admin
            If True, then fetch the user from the admin user management endpoint. This is the default
            if requesting a user other than the currently logged-in user

        Returns
        -------
        :
            Information about the user
        """

        if username is None:
            username = self.username

        if username is None:
            raise RuntimeError("Cannot get user - not logged in?")

        # Check client side so we can bail early
        is_valid_username(username)

        if username != self.username:
            as_admin = True

        if as_admin is False:
            # For the currently logged-in user, use the "me" endpoint. The other endpoint is
            # restricted to admins
            uinfo = self._auto_request("get", f"v1/me", None, None, UserInfo, None, None)

            if uinfo.username != self.username:
                raise RuntimeError(
                    f"Inconsistent username - client is {self.username} but logged in as {uinfo.username}"
                )
        else:
            uinfo = self._auto_request("get", f"v1/user/{username}", None, None, UserInfo, None, None)

        return uinfo

    def add_user(self, user_info: UserInfo, password: Optional[str] = None) -> str:
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

        is_valid_username(user_info.username)
        is_valid_rolename(user_info.role)

        if password is not None:
            is_valid_password(password)

        if user_info.id is not None:
            raise RuntimeError("Cannot add user when user_info contains an id")

        return self._auto_request(
            "post", "v1/user", Tuple[UserInfo, Optional[str]], None, str, (user_info, password), None
        )

    def modify_user(self, user_info: UserInfo, as_admin: bool = False) -> UserInfo:
        """
        Modifies a user on the server

        The user is determined by the username field of the input UserInfo, although the id
        and username are checked for consistency.

        Depending on the current user's permissions, some fields may not be updatable.



        Parameters
        ----------
        user_info
            Updated information for a user
        as_admin
            If True, then attempt to modify fields that are only modifiable by an admin (enabled, role).
            This is the default if requesting a user other than the currently logged-in user.

        Returns
        -------
        :
            The updated user information as it appears on the server
        """

        is_valid_username(user_info.username)
        is_valid_rolename(user_info.role)

        if as_admin or (user_info.username != self.username):
            url = f"v1/user/{user_info.username}"
        else:
            url = "v1/me"

        return self._auto_request("put", url, UserInfo, None, UserInfo, user_info, None)

    def change_user_password(self, username: Optional[str] = None, new_password: Optional[str] = None) -> str:
        """
        Change a users password

        If the username is not specified, then the current logged-in user is used.

        If the password is not specified, then one is automatically generated by the server.

        Parameters
        ----------
        username
            The name of the user whose password to change. If None, then use the currently logged-in user
        new_password
            Password to change to. If None, let the server generate one.

        Returns
        -------
        :
            The new password (either the same as the supplied one, or the server generated one
        """

        if username is None:
            username = self.username

        is_valid_username(username)

        if new_password is not None:
            is_valid_password(new_password)

        if username == self.username:
            url = "v1/me/password"
        else:
            url = f"v1/user/{username}/password"

        return self._auto_request("put", url, Optional[str], None, str, new_password, None)

    def delete_user(self, username: str) -> None:
        is_valid_username(username)

        if username == self.username:
            raise RuntimeError("Cannot delete your own user!")

        return self._auto_request("delete", f"v1/user/{username}", None, None, None, None, None)
