"""
Base declarative class for all ORM
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import inspect
from sqlalchemy.orm import as_declarative

if TYPE_CHECKING:
    from typing import Any, TypeVar, Type, Dict, Optional, Iterable, Union

    try:
        from pydantic.v1 import BaseModel
    except ImportError:
        from pydantic import BaseModel

    _T = TypeVar("_T")


@as_declarative()
class BaseORM:
    """Base declarative class of all ORM"""

    @classmethod
    def from_model(cls, model_data: Union[dict, BaseModel]):
        """
        Converts a pydantic model or dictionary to this ORM type

        By default, we just construct the ORM from the fields of the model. If they don't match one-to-one,
        then this function should be overridden by the derived class
        """

        if isinstance(model_data, dict):
            return cls(**model_data)
        else:
            return cls(**model_data.dict())

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        """
        Converts the ORM to a dictionary that corresponds to the QCPortal pydantic model

        All columns are included by default, but some can be removed using the exclude parameter.
        """

        d = self.__dict__.copy()
        d.pop("_sa_instance_state")

        # remove typical model excludes
        if hasattr(self, "_qcportal_model_excludes"):
            for k in self._qcportal_model_excludes:
                d.pop(k, None)

        # remove any manually specified excludes
        if exclude is not None:
            for k in exclude:
                d.pop(k, None)

        for k, v in d.items():
            if isinstance(v, BaseORM):
                d[k] = v.model_dict()
            elif isinstance(v, list):
                d[k] = [x.model_dict() if isinstance(x, BaseORM) else x for x in v]
            elif isinstance(v, dict):
                d[k] = {x: y.model_dict() if isinstance(y, BaseORM) else y for x, y in v.items()}

        return d

    def to_model(self, as_type: Type[_T]) -> _T:
        """
        Converts this ORM to a particular type or pydantic model

        This will convert this ORM to a type that has matching columns. For example,
        MoleculeORM to a Molecule

        Parameters
        ----------
        as_type
            Type to convert to

        Returns
        -------
        :
            An object of type Type
        """

        return as_type(**self.model_dict())

    def to_insert_dict(self) -> Dict[str, Any]:
        """
        Converts this model into a dictionary that can be inserted into the database

        This is useful for using SQLAlchemy constructs that take dictionaries rather than ORM objects.

        - Skips autoincrement PKs if None
        - Skips computed / server-generated columns
        """

        state = inspect(self)
        mapper = state.mapper
        table = mapper.local_table

        d = {}
        for col in table.columns:
            # Remove any autoincrement primary keys that are manually set to None. If set, then
            # it will try to be inserted into the db, resulting in a not-null violation
            if col.primary_key and col.autoincrement and getattr(self, col.name, None) is None:
                continue

            if col.computed:
                continue

            if hasattr(self, col.name):
                d[col] = getattr(self, col.name)

        return d
