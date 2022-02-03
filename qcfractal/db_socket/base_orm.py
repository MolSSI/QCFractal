from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.ext.declarative import as_declarative

if TYPE_CHECKING:
    from typing import Any, TypeVar, Type, Dict, Optional, Iterable

    _T = TypeVar("_T")


@as_declarative()
class BaseORM:
    """Base declarative class of all ORM models"""

    db_related_fields = ["result_type", "base_result_id", "_trajectory", "collection_type", "lname"]

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        """
        Converts the ORM to a dictionary

        All columns and hybrid properties are included by default, but some
        can be removed using the exclude parameter.

        The include_relations parameter specifies any relations to also
        include in the dictionary. By default, none will be included.

        NOTE: This is meant to replace to_dict above
        """

        d = self.__dict__.copy()
        d.pop("_sa_instance_state")

        if exclude is not None:
            for k in exclude:
                d.pop(k, None)

        if len(d) == 0:
            raise RuntimeError(
                "Dictionary of ORM is empty. It is likely that this ORM object is expired "
                "(ie, you are calling dict() after a session commit()) or you haven't specified "
                "any columns to be loaded. This is a QCFractal developer error."
            )

        for k, v in d.items():
            if isinstance(v, BaseORM):
                d[k] = v.dict()
            elif isinstance(v, list):
                d[k] = [x.dict() if isinstance(x, BaseORM) else x for x in v]
            elif isinstance(v, dict):
                d[k] = {x: y.dict() if isinstance(y, BaseORM) else y for x, y in v.items()}

        return d

    def _to_model(self, as_type: Type[_T]) -> _T:
        """
        Converts this ORM to a particular type

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

        return as_type(**self.dict())
