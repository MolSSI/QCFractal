#!/usr/bin/env python3

"""
A command line script to copy a sample of the production DB into staging or
development DB.

"""

import io
import psycopg2
import qcexport

from qcfractal.storage_sockets import storage_socket_factory

def create_destination_db(full_uri):
    '''Creates an empty database

    If the database already exists, an exception is raised

    '''
    # First, does the database exist?
    # Any better way to check if a db exists?
    #  https://dba.stackexchange.com/questions/45143/check-if-postgresql-database-exists-case-insensitive-way

    # Expected that the URI contains the dbname
    base_uri, dbname = full_uri.rsplit('/', maxsplit=1)

    print(f'* Testing if {dbname} exists in {base_uri}')

    conn = psycopg2.connect(base_uri)

    # Enable autocommit (to allow creating the db)
    # see https://stackoverflow.com/questions/34484066/create-a-postgres-database-using-python
    conn.autocommit = True

    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))

    # Check if destination db exists. If it does, abort
    exists = bool(cur.rowcount)
    if exists:
        raise RuntimeError(f"Database {dbname} exists! For safety, I will not overwrite")

    print(f'* Creating database {full_uri}')
    cur.execute(f"CREATE DATABASE {dbname}")


def copy_alembic_version(src_uri, dest_uri):
    ''' Copies the alembic_version table from the source to the destination '''

    print("* Copying alembic information")

    conn_src = psycopg2.connect(src_uri)
    conn_dest = psycopg2.connect(dest_uri)
    conn_dest.autocommit = True

    cur_src = conn_src.cursor()
    cur_dest = conn_dest.cursor()

    # We use a io.StringIO object, which behaves
    # like a file object that copy_to/copy_from expects
    alembic_data = io.StringIO()

    # Copy the data from the source
    cur_src.copy_to(alembic_data, 'alembic_version')

    # Place the internal pointer back to the beginning of the stream
    # (so that reads start from the beginning)
    alembic_data.seek(0)

    # Now create the table in the destination and copy the data there
    cur_dest.execute(f"CREATE TABLE alembic_version (version_num varchar(32) NOT NULL PRIMARY KEY)")
    cur_dest.copy_from(alembic_data, 'alembic_version')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Copy a sample of a production QCArchive database into a staging or development database')

    parser.add_argument('src', type=str, help='Source database (usually production)')
    parser.add_argument('dest', type=str, help='Destination database URI (must not exist)')
    parser.add_argument('-M', '--maxlimit', type=int, default=100000, help='Maximum value for a LIMIT')
    args = parser.parse_args()

    print("-"*80)
    print(f'     Source URI: {args.src}')
    print(f'Destination URI: {args.dest}')
    print("-"*80)
    print()

    # Connect to the source database
    production_storage = storage_socket_factory(args.src, db_type="sqlalchemy", max_limit=args.maxlimit, skip_version_check=False)

    # Create & connect to the destination database.
    # If it exists, this function should raise an exception
    create_destination_db(args.dest)
    copy_alembic_version(args.src, args.dest)

    # Set up the socket for the destination
    staging_storage = storage_socket_factory(args.dest, db_type="sqlalchemy", max_limit=args.maxlimit)

    ################################################################
    # START COPYING DATA
    ################################################################
    # indent stores the current output level. This allows for nesting
    # calls copying some lower-level tables
    indent = ''

    full_pk_map = {}
    options = {'dataset_max_entries': 2,
               'queue_manager_log_max': 50,
               'truncate_kv_store': False,
              }

    #############################
    # Start exporting stuff here
    #############################

    # Export everything from versions
    qcexport.general_copy('versions', staging_storage, production_storage, full_pk_map, options, {}, limit=None)

    # Errored Calculations. These should also have tasks and/or services
    qcexport.general_copy('result', staging_storage, production_storage, full_pk_map, options, {'status': 'error'}, {'id': 'desc'}, 10)
    qcexport.general_copy('optimization_procedure', staging_storage, production_storage, full_pk_map, options, {'status': 'error'}, {'id': 'desc'}, 10)
    qcexport.general_copy('grid_optimization_procedure', staging_storage, production_storage, full_pk_map, options, {'status': 'error'}, {'id': 'desc'}, 2)
    qcexport.general_copy('torsiondrive_procedure', staging_storage, production_storage, full_pk_map, options, {'status': 'error'}, {'id': 'desc'}, 2)

    # Datasets (Energy)
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '155'})

    # Datasets (Hessian)
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '262'})
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '159'})


    # OptimizationDataSet
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '50'})
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '253'})

    # GridOptimizationDataSet
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '237'})
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '239'})

    # ReactionDataSet
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '184'})
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '186'})

    # TorsiondriveDataSet
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '48'})
    qcexport.general_copy('collection', staging_storage, production_storage, full_pk_map, options, {'id': '217'})

    
