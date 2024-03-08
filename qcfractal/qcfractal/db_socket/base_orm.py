"""
Base declarative class for all ORM
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.orm import as_declarative

if TYPE_CHECKING:
    from typing import Any, TypeVar, Type, Dict, Optional, Iterable, Union, List

    try:
        from pydantic.v1 import BaseModel
    except ImportError:
        from pydantic import BaseModel

    _T = TypeVar("_T")


@as_declarative()
class BaseORM:
    """Base declarative class of all ORM"""

    @staticmethod
    def append_exclude(exclude: Optional[Iterable[str]] = None, *args: str) -> List[str]:
        """
        Appends additional exclude entries to a list

        The updated list is returned. If a list or other iterable is not given (ie, `exclude` is `None`)
        then one created.

        Parameters
        ----------
        exclude
            Existing list (or other iterable) of includes. This input may be modified.

        args
            Additional exclude entries to add to the list

        Returns
        -------
        :
            The input iterable as a list, with the additional entries added.
        """

        if exclude is None:
            return list(args)
        else:
            exclude = list(exclude)
            exclude.extend(args)
            return exclude

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
