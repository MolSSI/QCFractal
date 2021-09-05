'''Import/Export of QCArchive data
'''

from dataclasses import dataclass
import typing
from qcexport_extra import extra_children_map

from sqlalchemy.orm import make_transient, Load
from sqlalchemy import inspect

from qcfractal.storage_sockets.models import (
    AccessLogORM,
    BaseResultORM,
    CollectionORM,
    DatasetORM,
    GridOptimizationProcedureORM,
    KeywordsORM,
    KVStoreORM,
    OptimizationProcedureORM,
    QueueManagerLogORM,
    QueueManagerORM,
    ResultORM,
    ServerStatsLogORM,
    ServiceQueueORM,
    QueueManagerORM,
    TaskQueueORM,
    TorsionDriveProcedureORM,
    Trajectory,
    VersionsORM,
    WavefunctionStoreORM,
)
from qcfractal.components.molecule.db_models import MoleculeORM

from qcfractal.storage_sockets.models.collections_models import DatasetEntryORM
from qcfractal.storage_sockets.models.results_models import GridOptimizationAssociation, TorsionInitMol

_all_orm = [
    AccessLogORM,
    BaseResultORM,
    CollectionORM,
    DatasetORM,
    DatasetEntryORM,
    GridOptimizationProcedureORM,
    GridOptimizationAssociation,
    MoleculeORM,
    KeywordsORM,
    KVStoreORM,
    OptimizationProcedureORM,
    QueueManagerLogORM,
    QueueManagerORM,
    ResultORM,
    ServerStatsLogORM,
    ServiceQueueORM,
    QueueManagerORM,
    TaskQueueORM,
    TorsionDriveProcedureORM,
    TorsionInitMol,
    Trajectory,
    VersionsORM,
    WavefunctionStoreORM,
]

# Maps table names to sqlalchemy ORM objects
_table_orm_map = {orm.__tablename__: orm for orm in _all_orm}


class RowKeyValues:
    '''Generates and stores information about primary and foreign keys of a table
    '''
    @dataclass(order=True)
    class PKInfo:
        '''Holds information about a row's primary key.

        Holds the column names and the values of the primary key columns.
        These are lists in order to handle composite primary keys
        '''
        table: str
        columns: list
        values: list

    @dataclass(order=True)
    class FKInfo:
        '''Holds information about a row's foreign key.

        For a single foreign key, holds the source and destination/foreign table names and columns. Also
        holds the value in the source row.
        '''
        src_table: str
        src_column: str
        dest_table: str
        dest_column: str
        value: 'typing.Any'

    def __init__(self, orm_obj):
        '''Generates primary and foreign key info given an ORM object'''

        self.orm_type = type(orm_obj)
        insp = inspect(self.orm_type)

        ###########################################################
        # First, get which columns are primary and foreign keys
        ###########################################################

        # Handle if this is a derived class (polymorphic?)
        # This seems poorly documented. But get the table name of the
        # base class (if there is one)
        base_class = insp.inherits.entity if insp.inherits else None
        base_table = base_class.__tablename__ if base_class else None

        # Get the columns comprising the primary key
        primary_key_columns = [x.name for x in insp.primary_key]

        # Now foreign keys. Loop over all the columns.
        # Each column has a set() (which may be empty) stored in foreign_keys
        foreign_key_info = []
        for col in insp.columns:
            for fk in sorted(list(col.foreign_keys)):
                # Remove foreign keys to base class
                # The purpose of this function is to get foreign keys that we need to
                # load. But if it is part of the base class, then no need to do that
                if not (base_table and fk.column.table.name == base_table):
                    new_fk = self.FKInfo(col.table.name, col.name, fk.column.table.name, fk.column.name, None)
                    foreign_key_info.append(new_fk)

        # Not sure if order is always preserved, but sort just in case
        # so that things are always consistent
        primary_key_columns = sorted(primary_key_columns)
        foreign_key_info = sorted(foreign_key_info)

        # Now store in this class
        self.primary_key = self.PKInfo(self.orm_type.__tablename__, primary_key_columns, None)
        self.foreign_keys = foreign_key_info

        #######################################################
        # Obtain values for the primary and foreign key columns
        #######################################################
        self.primary_key.values = [getattr(orm_obj, column) for column in self.primary_key.columns]
        for fk in self.foreign_keys:
            fk.value = getattr(orm_obj, fk.src_column)

    def is_composite_primary(self):
        '''Returns True if this represents a composite primary key'''
        return len(self.primary_key.columns) > 1

    def as_lookup_key(self):
        '''Return a unique string representing the primary key

        This is used as a key to a dictionary to store already-copied data.
        '''
        return repr(self.orm_type) + repr(self.primary_key)

    def remove_primary_key(self, orm_obj):
        '''Remove primary key values that are integers and not part of
           a composite primary key'''

        if type(orm_obj) != self.orm_type:
            raise RuntimeError("Removing primary keys of type f{type(orm_obj)} but I can only handle {self.orm_type}")

        # Don't touch composite primary
        if self.is_composite_primary():
            return

        for pk, old_value in zip(self.primary_key.columns, self.primary_key.values):
            if isinstance(old_value, int):
                setattr(orm_obj, pk, None)


def _add_children(orm_obj, session_dest, session_src, new_pk_map, options, row_key_info, indent=''):
    '''Given an ORM object, adds the dependent data (through foreign keys)

    Finds all the foreign keys for the object, and adds the dependent data to the DB.
    It then fixes the values of the foreign keys in the ORM object to match the newly-inserted data.

    Parameters
    ----------
    orm_obj
        An ORM object to add the children of
    session_dest
        SQLAlchemy session to write data to
    session_src
        SQLAlchemy session to read data from
    new_pk_map : dict
        Where to store the mapping of old to new data
    options : dict
        Various options to be passed into the internal functions
    row_key_info : RowKeyValues
        Information about the row's primary and foreign keys
    indent : str
        Prefix to add to all printed output lines
    '''

    for fk_info in row_key_info.foreign_keys:
        # Data in that column may be empty/null
        if fk_info.value is None:
            continue

        print(indent + "+ Handling child: ")
        print(
            indent +
            f"  - {fk_info.src_table}.{fk_info.src_column}:{fk_info.value}  ->  {fk_info.dest_table}.{fk_info.dest_column}"
        )


        # We need to load from the db (from the foreign/destination table) given the column and value
        #    in the foreign key info
        fk_query = {fk_info.dest_column: fk_info.value}

        # Copy the foreign info. This should only return one record
        # NOTE: This requires going to the source db for info. It is possible that
        #       we can check new_pk_map here using the info from the foreign key to see if it
        #       was already done. However, the hit rate would generally be low, and might be error
        #       prone, especially with esoteric cases.
        new_info = _general_copy(table_name=fk_info.dest_table,
                                 session_dest=session_dest,
                                 session_src=session_src,
                                 new_pk_map=new_pk_map,
                                 options=options,
                                 filter_by=fk_query,
                                 single=True,
                                 indent=indent + '  ')

        # Now set the foreign keys to point to the new id
        setattr(orm_obj, fk_info.src_column, new_info[fk_info.dest_column])


def _add_tasks_and_services(base_result_id, session_dest, session_src, new_pk_map, options, indent):
    '''Adds entries in the task_queue and service_queue given something deriving from base_result

    Should only be called after adding the result or procedure.

    Parameters
    ----------
    base_result_id
        ID of the base_result (result, procedure, ...)
    session_dest
        SQLAlchemy session to write data to
    session_src
        SQLAlchemy session to read data from
    new_pk_map : dict
        Where to store the mapping of old to new data
    options : dict
        Various options to be passed into the internal functions
    indent : str
        Prefix to add to all printed output lines
    '''

    print(indent + f"$ Adding task & service queue entries for base_result_id = {base_result_id}")

    # Add anything from the task queue corresponding to the given base result id
    # (if calculation is completed, task is deleted)
    _general_copy(table_name='task_queue',
                  session_dest=session_dest,
                  session_src=session_src,
                  new_pk_map=new_pk_map,
                  options=options,
                  filter_by={'base_result_id': base_result_id},
                  indent=indent + '  ')

    # Do the same for the services queue
    #if int(base_result_id) == 17761750:
    #    breakpoint()
    _general_copy(table_name='service_queue',
                  session_dest=session_dest,
                  session_src=session_src,
                  new_pk_map=new_pk_map,
                  options=options,
                  filter_by={'procedure_id': base_result_id},
                  indent=indent + '  ')


def _general_copy(table_name,
                  session_dest,
                  session_src,
                  new_pk_map,
                  options,
                  filter_by=None,
                  filter_in=None,
                  order_by=None,
                  limit=None,
                  single=False,
                  indent=''):
    ''' 
    Given queries, copies all results of the query from session_src to session_dest

    Adds data to session_dest, keeping a map of newly-added info and fixing foreign keys
    to match newly-inserted data.
    
    Called recursively to add dependent data through foreign keys.

    Parameters
    ----------
    table_name : str
        Name of the table to copy data from/to
    session_dest
        SQLAlchemy session to write data to
    session_src
        SQLAlchemy session to read data from
    new_pk_map : dict
        Where to store the mapping of old to new data
    options : dict
        Various options to be passed into the internal functions
    filter_by : dict
        Filters (column: value) to add to the query. ie, {'id': 123}
    filter_in : dict
        Filters (column: list(values)) to add to the query using 'in'. ie, {'id': [123,456]}
    order_by: dict
        How to order the results of the query. ie {'id': 'desc'}
    limit : int
        Limit the number of records returned
    single : bool
        If true, expect only one returned record. If not, raise an exception
    indent : str
        Prefix to add to all printed output lines
    '''

    orm_type = _table_orm_map[table_name]

    # Build the query based on filtering, etc
    query = session_src.query(orm_type)

    if filter_by is not None:
        query = query.filter_by(**filter_by)

    if filter_in is not None:
        for key, values in filter_in.items():
            query = query.filter(getattr(orm_type, key).in_(values))

    if order_by:
        for column, order in order_by.items():
            # Gets, for example, Trajectory.opt_id.desc
            # opt_id = column, desc = bound function
            o = getattr(orm_type, column)
            o = getattr(o, order)

            query = query.order_by(o())

    if limit is not None:
        if single and limit != 1:
            raise RuntimeError(f'Limit = {limit} but single return is specified')
        query = query.limit(limit)
    elif single:
        limit = 1

    # Disable all relationship loading
    query = query.options(Load(orm_type).noload('*'))

    data = query.all()
    return_info = []

    # We have to expunge and make transient everything first
    # If not, sqlalchemy tries to be smart. After you add the entries found
    # through foreign keys, the rest of the objects in the data list may change.
    # But then you will have parts of objects in session_src and parts in session_dest
    for d in data:
        session_src.expunge(d)
        make_transient(d)

    for d in data:
        # Obtain primary/foreign key columns and values
        src_rck = RowKeyValues(d)

        # The type of the object may not be the same as we queried (due to polymorphic types)
        real_orm_type = type(d)
        real_table_name = real_orm_type.__tablename__

        # real_orm_type should never be BaseResultORM
        assert real_orm_type != BaseResultORM

        print(indent +
              f'* Copying {table_name} {str(src_rck.primary_key.columns)} = {str(src_rck.primary_key.values)}')

        if real_orm_type != orm_type:
            print(indent + f'& But actually using table {real_table_name}')
            
        ############################################################
        ############################################################
        ## TODO - If working with an existing db, do lookups here ##
        ##        (this is for future capability of importing     ##
        ##        into an existing db)                            ##
        ############################################################
        ############################################################

        src_lookup_key = src_rck.as_lookup_key()
        if src_lookup_key in new_pk_map:
            print(indent + f'  - Already previously done')
            return_info.append(new_pk_map[src_lookup_key])
            continue

        # Save src information for laters. When adding extra children, old ids and stuff may be needed
        src_info = d.to_dict()

        # Loop through foreign keys and recursively add those
        _add_children(d, session_dest, session_src, new_pk_map, options, src_rck, indent + '  ')

        # Remove the primary key. We will generate a new one on adding
        src_rck.remove_primary_key(d)

        # Truncate KV store entries by default
        # (but can be overridden)
        if table_name == 'kv_store':
            truncate_kv_store = options.get('truncate_kv_store', True)
            if truncate_kv_store:
                d.value = str(d.value)[:2000]
            

        # Now add it to the session
        # and obtain the key info
        session_dest.add(d)
        session_dest.commit()

        dest_rck = RowKeyValues(d)
        print(indent + f'! adding {real_table_name} {str(src_rck.primary_key.values)} = {str(dest_rck.primary_key.values)}')

        # Store the info for the entire row
        # (exception: kvstore)
        dest_info = d.to_dict()

        # Don't store kvstore data in the dictionary (not needed)
        if table_name == 'kv_store':
            dest_info.pop('value')

        # We can't just use primary key, since foreign keys may
        # reference non-primary-keys of other tables (as long as they are unique)
        new_pk_map[src_lookup_key] = dest_info
        return_info.append(dest_info)

        ########################################################################
        # Now handle children that are not specified by foreign keys
        # This includes decoupled data like datasets, as well as when foreign
        # keys are specified in json
        #
        # We do that here after adding. Some of these have foreign keys
        # to this object, so we need the new id (retrieved through new_pk_map)
        ########################################################################
        if real_orm_type in extra_children_map:
            # The function called in extra_children_map may modify the object.
            # We let the called function do that, then merge it back into the db
            extra_children_map[real_orm_type](d, src_info, session_dest, session_src, new_pk_map, options, indent + '  ')
            session_dest.commit()

        ########################################################################
        # Now add tasks/services if this is a result/procedure
        ########################################################################
        if issubclass(real_orm_type, BaseResultORM):
            _add_tasks_and_services(src_info['id'], session_dest, session_src, new_pk_map, options, indent + '  ')

    # If the caller specified single=True, should only be one record
    if single:
        if len(return_info) != 1:
            raise RuntimeError(f'Wanted single record but got {len(return_info)} instead')
        return return_info[0]
    else:
        return return_info


def general_copy(table_name,
                 storage_dest,
                 storage_src,
                 new_pk_map=None,
                 options={},
                 filter_by={},
                 order_by=None,
                 limit=None,
                 indent=''):
    ''' Copies data from the source db to the destination db

    Given queries, copies all results of the query from session_src to session_dest

    Handles copying of data required by foreign keys as well.

    Parameters
    ----------
    table_name : str
        Name of the table to copy data from/to
    storage_dest
        Storage object to write data to
    storage_src
        Storage object to read data from
    new_pk_map : dict
        Where to store the mapping of old to new data
    options : dict
        Various options to be passed into the internal functions
    filter_by : dict
        Filters (column: value) to add to the query. ie, {'id': 123}
    order_by: dict
        How to order the results of the query. ie {'id': 'desc'}
    limit : int
        Limit the number of records returned
    indent : str
        Prefix to add to all printed output lines
    '''

    if new_pk_map is None:
        new_pk_map = dict()

    with storage_src.session_scope() as session_src:
        with storage_dest.session_scope() as session_dest:
            _general_copy(table_name,
                          session_dest,
                          session_src,
                          new_pk_map=new_pk_map,
                          options=options,
                          filter_by=filter_by,
                          order_by=order_by,
                          limit=limit,
                          indent=indent)
