from __future__ import annotations

import msgpack

from qcfractal.interface.models import ObjectId
from qcelemental.util import msgpackext_dumps, msgpackext_loads
from sqlalchemy import and_, inspect, Integer
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.ext.associationproxy import ASSOCIATION_PROXY
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.hybrid import HYBRID_PROPERTY
from sqlalchemy.orm import object_session
from sqlalchemy.types import TypeDecorator

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import str, Any, TypeVar, Type, Dict

    _T = TypeVar("_T")


class MsgpackExt(TypeDecorator):
    """Converts JSON-like data to msgpack with full NumPy Array support."""

    impl = BYTEA

    # I believe caching is only used when, for example, you filter by a column. But we
    # shouldn't ever do that with msgpack
    cache_ok = False

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpackext_dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpackext_loads(value)


class PlainMsgpackExt(TypeDecorator):
    """Converts JSON-like data to msgpack using standard msgpack

    This does not support NumPy"""

    impl = BYTEA

    # I believe caching is only used when, for example, you filter by a column. But we
    # shouldn't ever do that with msgpack
    cache_ok = False

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpack.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpack.loads(value)


@as_declarative()
class Base:
    """Base declarative class of all ORM models"""

    db_related_fields = ["result_type", "base_result_id", "_trajectory", "collection_type", "lname"]

    def to_dict(self, exclude=None):

        tobe_deleted_keys = []

        if exclude:
            tobe_deleted_keys.extend(exclude)

        dict_obj = [x for x in self._all_col_names() if x not in self.db_related_fields and x not in tobe_deleted_keys]

        # Add the attributes to the final results
        ret = {k: getattr(self, k) for k in dict_obj}

        if "extra" in ret:
            ret.update(ret["extra"])
            del ret["extra"]

        # TODO - INT ID we shouldn't be doing this
        # transform ids from int into ObjectId
        id_fields = self._get_fieldnames_with_DB_ids_()
        for key in id_fields:
            if key in ret.keys() and ret[key] is not None:
                if isinstance(ret[key], (list, tuple)):
                    ret[key] = [ObjectId(i) for i in ret[key]]
                else:
                    ret[key] = ObjectId(ret[key])

        return ret

    def dict(self) -> Dict[str, Any]:
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

        if len(d) == 0:
            raise RuntimeError(
                "Dictionary of ORM is empty. It is likely that this ORM object is expired (ie, you are calling dict() after a session commit()) or you haven't specified any columns to be loaded. This is a QCFractal developer error."
            )

        for k, v in d.items():
            if isinstance(v, Base):
                d[k] = v.dict()
            elif isinstance(v, list):
                d[k] = [x.dict() if isinstance(x, Base) else x for x in v]

        return d

    def to_model(self, as_type: Type[_T]) -> _T:
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

    @classmethod
    def _get_fieldnames_with_DB_ids_(cls):

        class_inspector = inspect(cls)
        id_fields = []
        for key, col in class_inspector.columns.items():
            # if PK, FK, or column property (TODO: work around for column property)
            if col.primary_key or len(col.foreign_keys) > 0 or key != col.name:
                id_fields.append(key)

        return id_fields

    @classmethod
    def get_autoincrement_pkey(cls):
        """
        Returns the name of the primary key column with an autoincrement/serial id. If there
        isn't one, None is returned
        """

        if hasattr(cls, "__autoincrement_pkey"):
            return cls.__autoincrement_pkey

        pk_cols = inspect(cls).primary_key

        # Composite primary key? Don't think we use those
        assert len(pk_cols) <= 1

        if len(pk_cols) == 0:
            cls.__autoincrement_pkey = None

        pk_col = pk_cols[0]

        # To be autoincrement/serial, the column must be an integer type (or derived from that),
        # and the autoincrement must be set to 'auto' (default) or explicitly set to True
        if issubclass(type(pk_col.type), Integer) and pk_col.autoincrement in ["auto", True]:
            cls.__autoincrement_pkey = pk_col.name
        else:
            # Found a primary key, but is not autoincrement and integer
            cls.__autoincrement_pkey = None

        return cls.__autoincrement_pkey

    @classmethod
    def _get_col_types(cls):

        # Must use private attributes so that they are not shared by subclasses
        if hasattr(cls, "__columns") and hasattr(cls, "__hybrids") and hasattr(cls, "__relationships"):
            return cls.__columns, cls.__hybrids, cls.__relationships

        mapper = inspect(cls)

        cls.__columns = []
        cls.__hybrids = []
        cls.__relationships = {}
        for k, v in mapper.relationships.items():
            cls.__relationships[k] = {}
            cls.__relationships[k]["join_class"] = v.argument
            cls.__relationships[k]["remote_side_column"] = list(v.remote_side)[0]

        for k, c in mapper.all_orm_descriptors.items():

            if k == "__mapper__":
                continue

            if c.extension_type == ASSOCIATION_PROXY:
                continue

            if c.extension_type == HYBRID_PROPERTY:
                cls.__hybrids.append(k)
            elif k not in mapper.relationships:
                cls.__columns.append(k)

        return cls.__columns, cls.__hybrids, cls.__relationships

    @classmethod
    def get_col_types_2(cls):

        mapper = inspect(cls)

        columns = set(mapper.column_attrs.keys())
        relationships = set(mapper.relationships.keys())

        return columns, relationships

    @classmethod
    def _all_col_names(cls):
        all_cols, hybrid, _ = cls._get_col_types()
        return all_cols + hybrid

    def _update_many_to_many(self, table, parent_id_name, child_id_name, parent_id_val, new_list, old_list=None):
        """Perfomr upsert on a many to many association table
        Does NOT commit changes, parent should optimize when it needs to commit
        raises exception if ids don't exist in the DB
        """

        session = object_session(self)

        old_set = {x for x in old_list} if old_list else set()
        new_set = {x for x in new_list} if new_list else set()

        # Update many-to-many relations
        # Remove old relations and apply the new ones
        if old_set != new_set:
            to_add = new_set - old_set
            to_del = old_set - new_set

            if to_del:
                session.execute(
                    table.delete().where(
                        and_(table.c[parent_id_name] == parent_id_val, table.c[child_id_name].in_(to_del))
                    )
                )
            if to_add:
                session.execute(
                    table.insert().values([{parent_id_name: parent_id_val, child_id_name: my_id} for my_id in to_add])
                )

    def __str__(self):
        if hasattr(self, "id"):
            return str(self.id)
        return super.__str__(self)

    # @validates('created_on', 'modified_on')
    # def validate_date(self, key, date):
    #     """For SQLite, translate str to dates manually"""
    #     if date is not None and isinstance(date, str):
    #         date = dateutil.parser.parse(date)
    #     return date
