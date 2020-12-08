from pydantic.dataclasses import dataclass
import dataclasses
from typing import List, Optional, Tuple


@dataclass
class UpsertMetadata:
    """
    Metadata returned by upsert_* functions
    """

    # Integers in errors, inserted, existing are indices in the input/output list
    error_description: Optional[bool] = None
    errors: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
    inserted_idx: List[int] = dataclasses.field(default_factory=list)  # inserted into the db
    updated_idx: List[int] = dataclasses.field(default_factory=list)  # existing and updated

    @property
    def n_inserted(self):
        return len(self.inserted_idx)

    @property
    def n_updated(self):
        return len(self.updated_idx)

    @property
    def error_idx(self):
        return [x[0] for x in self.errors]

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0


@dataclass
class InsertMetadata:
    """
    Metadata returned by insert_* functions
    """

    # Integers in errors, inserted, existing are indices in the input/output list
    error_description: Optional[bool] = None
    errors: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
    inserted_idx: List[int] = dataclasses.field(default_factory=list)  # inserted into the db
    existing_idx: List[int] = dataclasses.field(default_factory=list)  # existing but not updated

    @property
    def n_inserted(self):
        return len(self.inserted_idx)

    @property
    def n_existing(self):
        return len(self.existing_idx)

    @property
    def error_idx(self):
        return [x[0] for x in self.errors]

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0


@dataclass
class GetMetadata:
    """
    Metadata returned by get_* functions
    """

    # Integers in errors, missing, found are indices in the input/output list
    error_description: Optional[bool] = None
    errors: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
    missing_idx: List[int] = dataclasses.field(default_factory=list)
    found_idx: List[int] = dataclasses.field(default_factory=list)

    @property
    def n_found(self):
        return len(self.found_idx)

    @property
    def n_missing(self):
        return len(self.missing_idx)

    @property
    def error_idx(self):
        return [x[0] for x in self.errors]

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0


@dataclass
class DeleteMetadata:
    """
    Metadata returned by delete_* functions
    """

    # Integers in errors, missing, found are indices in the input/output list
    error_description: Optional[bool] = None
    errors: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
    deleted_idx: List[int] = dataclasses.field(default_factory=list)
    missing_idx: List[int] = dataclasses.field(default_factory=list)

    @property
    def n_deleted(self):
        return len(self.deleted_idx)

    @property
    def n_missing(self):
        return len(self.missing_idx)

    @property
    def error_idx(self):
        return [x[0] for x in self.errors]

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0
