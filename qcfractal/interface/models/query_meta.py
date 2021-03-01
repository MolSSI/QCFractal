import dataclasses
from pydantic.dataclasses import dataclass
from pydantic import validator, root_validator
from typing import List, Optional, Tuple, Dict, Any


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


@dataclass
class DeleteMetadata:
    """
    Metadata returned by delete functions
    """

    # Integers in errors, missing, found are indices in the input/output list
    error_description: Optional[str] = None
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

    @validator("errors", "deleted_idx", "missing_idx", pre=True)
    def sort_fields(cls, v):
        return sorted(v)

    @root_validator(pre=False, skip_on_failure=True)
    def check_all_indices(cls, values):
        # Test that all indices are accounted for and that the same index doesn't show up in
        # deleted_idx, missing_idx, or errors
        del_idx = set(values["deleted_idx"])
        missing_idx = set(values["missing_idx"])
        error_idx = set(x[0] for x in values["errors"])

        if not del_idx.isdisjoint(missing_idx):
            intersection = del_idx.intersection(missing_idx)
            raise ValueError(f"inserted_idx and missing_idx are not disjoint: intersection={intersection}")

        if not del_idx.isdisjoint(error_idx):
            intersection = del_idx.intersection(error_idx)
            raise ValueError(f"inserted_idx and error_idx are not disjoint: intersection={intersection}")

        if not missing_idx.isdisjoint(error_idx):
            intersection = missing_idx.intersection(error_idx)
            raise ValueError(f"missing_idx and error_idx are not disjoint: intersection={intersection}")

        all_idx = del_idx | missing_idx | error_idx

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
class QueryMetadata:
    """
    Metadata returned by query functions
    """

    # Integers in errors, missing, found are indices in the input/output list
    error_description: Optional[str] = None
    errors: List[str] = dataclasses.field(default_factory=list)
    n_found: int = 0  # Total number found
    n_returned: int = 0  # How many we are actually returning (ie, the query has hit a limit)

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

    @property
    def n_errors(self):
        return len(self.errors)

    @validator("errors", pre=True)
    def sort_fields(cls, v):
        return sorted(v)

    def dict(self) -> Dict[str, Any]:
        """
        Returns the information from this dataclass as a dictionary
        """

        return dataclasses.asdict(self)


# @dataclass
# class UpsertMetadata:
#    """
#    Metadata returned by upsert_* functions
#    """
#
#    # Integers in errors, inserted, existing are indices in the input/output list
#    error_description: Optional[bool] = None
#    errors: List[Tuple[int, str]] = dataclasses.field(default_factory=list)
#    inserted_idx: List[int] = dataclasses.field(default_factory=list)  # inserted into the db
#    updated_idx: List[int] = dataclasses.field(default_factory=list)  # existing and updated
#
#    @property
#    def n_inserted(self):
#        return len(self.inserted_idx)
#
#    @property
#    def n_updated(self):
#        return len(self.updated_idx)
#
#    @property
#    def error_idx(self):
#        return [x[0] for x in self.errors]
#
#    @property
#    def success(self):
#        return self.error_description is None and len(self.errors) == 0
