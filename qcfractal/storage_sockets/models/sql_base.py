# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect, and_
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import object_session
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.hybrid import hybrid_property
# from sqlalchemy.ext.orderinglist import ordering_list
# from sqlalchemy.ext.associationproxy import association_proxy
# from sqlalchemy.dialects.postgresql import aggregate_order_by

from qcelemental.util import msgpackext_dumps, msgpackext_loads

# Base = declarative_base()

class MsgpackExt(TypeDecorator):
    '''Converts JSON-like data to msgpack with full NumPy Array support.'''

    impl = BYTEA

    def process_bind_param(self, value, dialect):
        return msgpackext_dumps(value)

    def process_result_value(self, value, dialect):
        return msgpackext_loads(value)


@as_declarative()
class Base:
    """Base declarative class of all ORM models"""

    db_related_fields = ['result_type', 'base_result_id', 'metadata', '_trajectory',
                         'collection_type']

    def to_dict(self, exclude=None):

        tobe_deleted_keys = []

        if exclude:
            tobe_deleted_keys.extend(exclude)

        dict_obj = [
            x for x in self.__dict__ if not x.startswith('_') and x not in self.db_related_fields
            and not x.endswith('_obj') and x not in tobe_deleted_keys
        ]

        class_inspector = inspect(self.__class__)
        # add hybrid properties
        for key, prop in class_inspector.all_orm_descriptors.items():
            if isinstance(prop, hybrid_property):
                dict_obj.append(key)

        # Add the attributes to the final results
        ret = {k: getattr(self, k) for k in dict_obj}

        if 'extra' in ret:
            ret.update(ret['extra'])
            del ret['extra']

        # transform ids from int into str
        id_fields = self._get_fieldnames_with_DB_ids_(class_inspector)
        for key in id_fields:
            if key in ret.keys() and ret[key] is not None:
                if isinstance(ret[key], (list, tuple)):
                    ret[key] = [str(i) for i in ret[key]]
                else:
                    ret[key] = str(ret[key])

        return ret

    @classmethod
    def _get_fieldnames_with_DB_ids_(cls, class_inspector=None):
        if not class_inspector:
            class_inspector = inspect(cls)
        id_fields = []
        for key, col in class_inspector.columns.items():
            # if PK, FK, or column property (TODO: work around for column property)
            if col.primary_key or len(col.foreign_keys) > 0 or key != col.key:
                id_fields.append(key)

        return id_fields

    @classmethod
    def col(cls):
        return [col for col in cls.__dict__ if not col.startswith('_')]

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
                session.execute(table.delete().where(
                    and_(table.c[parent_id_name] == parent_id_val, table.c[child_id_name].in_(to_del))))
            if to_add:
                session.execute(
                    table.insert()\
                        .values([(parent_id_val, my_id) for my_id in to_add])
                )

    def __str__(self):
        if hasattr(self, 'id'):
            return str(self.id)
        return super.__str__(self)

    # @validates('created_on', 'modified_on')
    # def validate_date(self, key, date):
    #     """For SQLite, translate str to dates manually"""
    #     if date is not None and isinstance(date, str):
    #         date = dateutil.parser.parse(date)
    #     return date
