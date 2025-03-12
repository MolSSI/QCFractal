from __future__ import annotations

import dataclasses
from typing import List, Optional, Tuple, Dict, Sequence, Any

try:
    from pydantic.v1 import validator, root_validator
    from pydantic.v1.dataclasses import dataclass
except ImportError:
    from pydantic import validator, root_validator
    from pydantic.dataclasses import dataclass


@dataclass
class InsertMetadata:
    """
    Metadata returned by insertion / adding functions
    """

    # Integers in errors, inserted, existing are indices in the input/output list
    error_description: Optional[str] = None
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
    def n_errors(self):
        return len(self.errors)

    @property
    def error_idx(self):
        return [x[0] for x in self.errors]

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0

    @property
    def error_string(self):
        s = ""
        if self.error_description:
            s += self.error_description + "\n"
        s += "\n".join(f"    Index {x}: {y}" for x, y in self.errors)
        return s

    @validator("errors", "inserted_idx", "existing_idx", pre=True)
    def sort_fields(cls, v):
        return sorted(v)

    @root_validator(pre=False, skip_on_failure=True)
    def check_all_indices(cls, values):
        # Test that all indices are accounted for and that the same index doesn't show up in
        # inserted_idx, existing_idx, or errors
        ins_idx = set(values["inserted_idx"])
        existing_idx = set(values["existing_idx"])
        error_idx = set(x[0] for x in values["errors"])

        if not ins_idx.isdisjoint(existing_idx):
            intersection = ins_idx.intersection(existing_idx)
            raise ValueError(f"inserted_idx and existing_idx are not disjoint: intersection={intersection}")

        if not ins_idx.isdisjoint(error_idx):
            intersection = ins_idx.intersection(error_idx)
            raise ValueError(f"inserted_idx and error_idx are not disjoint: intersection={intersection}")

        if not existing_idx.isdisjoint(error_idx):
            intersection = existing_idx.intersection(error_idx)
            raise ValueError(f"existing_idx and error_idx are not disjoint: intersection={intersection}")

        all_idx = ins_idx | existing_idx | error_idx

        # Skip the rest if we don't have any data
        if len(all_idx) == 0:
            return values

        # Are all the indices accounted for?
        all_possible = set(range(max(all_idx) + 1))
        if all_idx != all_possible:
            missing = all_possible - all_idx
            raise ValueError(f"All indices are not accounted for. Max is {max(all_idx)} and we are missing {missing}")

        return values

    def dict(self) -> Dict[str, Any]:
        """
        Returns the information from this dataclass as a dictionary
        """

        return dataclasses.asdict(self)

    @staticmethod
    def merge(metadata: Sequence[InsertMetadata]) -> InsertMetadata:
        new_inserted_idx: List[int] = []
        new_existing_idx: List[int] = []
        new_errors: List[Tuple[int, str]] = []
        new_error_description: Optional[str] = None

        base_idx = 0
        for m in metadata:
            new_inserted_idx.extend(i + base_idx for i in m.inserted_idx)
            new_existing_idx.extend(i + base_idx for i in m.existing_idx)
            new_errors.extend((i + base_idx, e) for i, e in m.errors)
            if m.error_description is not None:
                if new_error_description is None:
                    new_error_description = m.error_description
                else:
                    new_error_description += "\n" + m.error_description

            base_idx += len(m.inserted_idx) + len(m.existing_idx) + len(m.errors)

        return InsertMetadata(
            inserted_idx=new_inserted_idx,
            existing_idx=new_existing_idx,
            errors=new_errors,
            error_description=new_error_description,
        )


@dataclass
class InsertCountsMetadata:
    """
    Metadata returned by insertion / adding functions, only including counts
    """

    # Integers in errors, inserted, existing are indices in the input/output list
    n_inserted: int
    n_existing: int
    error_description: Optional[str] = None
    errors: List[str] = dataclasses.field(default_factory=list)

    @property
    def n_errors(self):
        return len(self.errors)

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0

    @property
    def error_string(self):
        s = ""
        if self.error_description:
            s += self.error_description + "\n"
        s += "\n".join(f"    Index {x}: {y}" for x, y in self.errors)
        return s

    def dict(self) -> Dict[str, Any]:
        """
        Returns the information from this dataclass as a dictionary
        """

        return dataclasses.asdict(self)

    @staticmethod
    def from_insert_metadata(insert_meta: InsertMetadata) -> InsertCountsMetadata:
        return InsertCountsMetadata(
            n_inserted=insert_meta.n_inserted,
            n_existing=insert_meta.n_existing,
            error_description=insert_meta.error_description,
            errors=[e for _, e in insert_meta.errors],
        )


@dataclass
class DeleteMetadata:
    """
    Metadata returned by delete functions
    """

    # Integers in errors, missing, found are indices in the input/output list
    error_description: Optional[str] = None
    errors: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
    deleted_idx: List[int] = dataclasses.field(default_factory=list)
    n_children_deleted: int = 0

    @property
    def n_deleted(self):
        return len(self.deleted_idx)

    @property
    def n_errors(self):
        return len(self.errors)

    @property
    def error_idx(self):
        return [x[0] for x in self.errors]

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0

    @property
    def error_string(self):
        s = ""
        if self.error_description:
            s += self.error_description + "\n"
        s += "\n".join(f"    Index {x}: {y}" for x, y in self.errors)
        return s

    @validator("errors", "deleted_idx", pre=True)
    def sort_fields(cls, v):
        return sorted(v)

    @root_validator(pre=False, skip_on_failure=True)
    def check_all_indices(cls, values):
        # Test that all indices are accounted for and that the same index doesn't show up in
        # deleted_idx, or errors
        del_idx = set(values["deleted_idx"])
        error_idx = set(x[0] for x in values["errors"])

        if not del_idx.isdisjoint(error_idx):
            intersection = del_idx.intersection(error_idx)
            raise ValueError(f"deleted_idx and error_idx are not disjoint: intersection={intersection}")

        all_idx = del_idx | error_idx

        # Skip the rest if we don't have any data
        if len(all_idx) == 0:
            return values

        # Are all the indices accounted for?
        all_possible = set(range(max(all_idx) + 1))
        if all_idx != all_possible:
            missing = all_possible - all_idx
            raise ValueError(f"All indices are not accounted for. Max is {max(all_idx)} and we are missing {missing}")

        return values

    def dict(self) -> Dict[str, Any]:
        """
        Returns the information from this dataclass as a dictionary
        """

        return dataclasses.asdict(self)


@dataclass
class UpdateMetadata:
    """
    Metadata returned by update functions
    """

    # Integers in errors, updated_idx
    error_description: Optional[str] = None
    errors: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
    updated_idx: List[int] = dataclasses.field(default_factory=list)  # inserted into the db
    n_children_updated: int = 0

    @property
    def n_updated(self):
        return len(self.updated_idx)

    @property
    def n_errors(self):
        return len(self.errors)

    @property
    def error_idx(self):
        return [x[0] for x in self.errors]

    @property
    def success(self):
        return self.error_description is None and len(self.errors) == 0

    @property
    def error_string(self):
        s = ""
        if self.error_description:
            s += self.error_description + "\n"
        s += "\n".join(f"    Index {x}: {y}" for x, y in self.errors)
        return s

    @validator("errors", "updated_idx", pre=True)
    def sort_fields(cls, v):
        return sorted(v)

    @root_validator(pre=False, skip_on_failure=True)
    def check_all_indices(cls, values):
        # Test that all indices are accounted for and that the same index doesn't show up in
        # inserted_idx, existing_idx, or errors
        upd_idx = set(values["updated_idx"])
        error_idx = set(x[0] for x in values["errors"])

        if not upd_idx.isdisjoint(error_idx):
            intersection = upd_idx.intersection(error_idx)
            raise ValueError(f"updated_idx and error_idx are not disjoint: intersection={intersection}")

        all_idx = upd_idx | error_idx

        # Skip the rest if we don't have any data
        if len(all_idx) == 0:
            return values

        # Are all the indices accounted for?
        all_possible = set(range(max(all_idx) + 1))
        if all_idx != all_possible:
            missing = all_possible - all_idx
            raise ValueError(f"All indices are not accounted for. Max is {max(all_idx)} and we are missing {missing}")

        return values

    def dict(self) -> Dict[str, Any]:
        """
        Returns the information from this dataclass as a dictionary
        """

        return dataclasses.asdict(self)


@dataclass
class TaskReturnMetadata:
    """
    Metadata returned to managers that have sent completed tasks back to the server
    """

    # Integers in errors, accepted_ids are task ids
    error_description: Optional[str] = None
    rejected_info: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
    accepted_ids: List[int] = dataclasses.field(default_factory=list)  # Accepted by the server

    @property
    def n_accepted(self):
        return len(self.accepted_ids)

    @property
    def n_rejected(self):
        return len(self.rejected_ids)

    @property
    def rejected_ids(self):
        return [x[0] for x in self.rejected_info]

    @property
    def success(self):
        return self.error_description is None

    @property
    def error_string(self):
        s = ""
        if self.error_description:
            s += self.error_description + "\n"
        s += "\n".join(f"    Task id {x}: {y}" for x, y in self.rejected_info)
        return s

    @validator("rejected_info", "accepted_ids", pre=True)
    def sort_fields(cls, v):
        return sorted(v)

    def dict(self) -> Dict[str, Any]:
        """
        Returns the information from this dataclass as a dictionary
        """

        return dataclasses.asdict(self)
