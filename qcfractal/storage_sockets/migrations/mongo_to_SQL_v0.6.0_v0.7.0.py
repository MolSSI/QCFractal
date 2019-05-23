"""
A command line script to migrate the mongo DB in version 0.6.0 to the
SQL DB in version 0.7.0

"""

import argparse
from qcfractal.storage_sockets import storage_socket_factory
from qcfractal.storage_sockets.me_models import (MoleculeORM, KeywordsORM, KVStoreORM, ResultORM,
                                                 OptimizationProcedureORM, ProcedureORM)
from qcfractal.storage_sockets.sql_models import (MoleculeMap, KeywordsMap, KVStoreMap, ResultMap,
                                                  OptimizationMap)
from qcfractal.interface.models import KeywordSet, ResultRecord, OptimizationRecord


sql_uri = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/qcarchivedb"
mongo_uri = "mongodb://localhost:27017"
mongo_db_name = "qcf_compute_server_test"
MAX_LIMIT = 100


def connect_to_DBs(mongo_uri, sql_uri, mongo_db_name, max_limit):

    mongo_storage = storage_socket_factory(mongo_uri, mongo_db_name, db_type="mongoengine",
                                           max_limit=max_limit)

    sql_storage = storage_socket_factory(sql_uri, 'qcarchivedb', db_type='sqlalchemy',
                                         max_limit=max_limit)

    print("DB limit: ", max_limit)

    return mongo_storage, sql_storage


def copy_molecules(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(MoleculeORM)
    print('Total # of Molecules in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_molecules(limit=max_limit, skip=skip)
        mongo_res = ret['data']
        print('mongo mol returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        with sql_storage.session_scope() as session:
            if session.query(MoleculeMap).filter_by(mongo_id=mongo_res[-1].id).count() > 0:
                print('Skipping first ', skip+max_limit)
                continue

        sql_insered = sql_storage.add_molecules(mongo_res)['data']
        print('Inserted in SQL:', len(sql_insered))

        if with_check:
            ret = sql_storage.get_molecules(limit=max_limit, skip=skip)
            print('Get from SQL: n_found={}, returned={}'.format(ret['meta']['n_found'], len(ret['data'])))

            assert mongo_res[0].compare(ret['data'][0])

        # store the ids mapping in the sql DB
        with sql_storage.session_scope() as session:
            obj_map = []
            for mongo_obj, sql_id in zip(mongo_res, sql_insered):
                obj_map.append(MoleculeMap(sql_id=sql_id, mongo_id=mongo_obj.id))

            session.add_all(obj_map)
            session.commit()

    print('---- Done copying molecules\n\n')


def copy_keywords(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    mongo_storage.add_keywords([KeywordSet(values={'key': 'test data'})])

    total_count = mongo_storage.get_total_count(KeywordsORM)
    print('Total # of Keywords in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_keywords(limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        with sql_storage.session_scope() as session:
            if session.query(KeywordsMap).filter_by(mongo_id=mongo_res[-1].id).count() > 0:
                print('Skipping first ', skip+max_limit)
                continue

        sql_insered = sql_storage.add_keywords(mongo_res)['data']
        print('Inserted in SQL:', len(sql_insered))

        if with_check:
            ret = sql_storage.get_keywords(limit=max_limit, skip=skip)
            print('Get from SQL: n_found={}, returned={}'.format(ret['meta']['n_found'], len(ret['data'])))

            assert mongo_res[0].hash_index == ret['data'][0].hash_index

        # store the ids mapping in the sql DB
        with sql_storage.session_scope() as session:
            obj_map = []
            for mongo_obj, sql_id in zip(mongo_res, sql_insered):
                obj_map.append(KeywordsMap(sql_id=sql_id, mongo_id=mongo_obj.id))

            session.add_all(obj_map)
            session.commit()

    print('---- Done copying keywords\n\n')


def copy_kv_store(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(KVStoreORM)
    print('Total # of KV_store in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_kvstore(limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo kv_store returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        ids = list(mongo_res.keys())
        values = mongo_res.values()
        # check if this patch has been already stored
        with sql_storage.session_scope() as session:
            if session.query(KVStoreMap).filter_by(mongo_id=ids[-1]).count() > 0:
                print('Skipping first ', skip+max_limit)
                continue

        sql_insered = sql_storage.add_kvstore(values)['data']
        print('Inserted in SQL:', len(sql_insered))

        if with_check:
            ret = sql_storage.get_kvstore(limit=max_limit, skip=skip)
            print('Get from SQL: n_found={}, returned={}'.format(ret['meta']['n_found'], len(ret['data'])))

            assert list(values)[0] == list(ret['data'].values())[0]

        # store the ids mapping in the sql DB
        with sql_storage.session_scope() as session:
            obj_map = []
            for mongo_id, sql_id in zip(ids, sql_insered):
                obj_map.append(KVStoreMap(sql_id=sql_id, mongo_id=mongo_id))

            session.add_all(obj_map)
            session.commit()

    print('---- Done copying KV_store\n\n')


def map_ids(sql_storage, field_names, className, mongo_res):
    # load mapped ids in memory
    ids_map = []
    for res in mongo_res:
        for field in field_names:
            if field in res and res[field]:
                ids_map.append(res[field])

    with sql_storage.session_scope() as session:
        objs = session.query(className).filter(className.mongo_id.in_(ids_map)).all()
        ids_map = {i.mongo_id:i.sql_id for i in objs}

    # replace mongo ids Results with sql
    for res in mongo_res:
        for field in field_names:
            if field in res and res[field]:
                res[field] = ids_map[res[field]]

    return mongo_res


def copy_results(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ResultORM)
    print('Total # of Results in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_results(limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        with sql_storage.session_scope() as session:
            if session.query(ResultMap).filter_by(mongo_id=mongo_res[-1]['id']).count() > 0:
                print('Skipping first ', skip+max_limit)
                continue

        # load mapped ids in memory
        mongo_res = map_ids(sql_storage, ['molecule'], MoleculeMap, mongo_res)
        mongo_res = map_ids(sql_storage, ['keywords'], KeywordsMap, mongo_res)
        mongo_res = map_ids(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)

        results_py = [ResultRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_results(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        with sql_storage.session_scope() as session:
            obj_map = []
            for mongo_obj, sql_id in zip(mongo_res, sql_insered):
                obj_map.append(ResultMap(sql_id=sql_id, mongo_id=mongo_obj['id']))

            session.add_all(obj_map)
            session.commit()

        if with_check:
            with sql_storage.session_scope() as session:
                res = session.query(ResultMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

            ret = sql_storage.get_results(id=[res])
            print('Get from SQL:', ret['data'])

            ret2 = mongo_storage.get_results(id=[mongo_res[0]['id']])
            print('Get from Mongo:', ret2['data'])
            assert ret2['data'][0]['return_result'] == ret['data'][0]['return_result']

def copy_optimization_procedure(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ProcedureORM, procedure='optimization')
    print('Total # of Optimization in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_procedures(procedure='optimization', limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        with sql_storage.session_scope() as session:
            if session.query(OptimizationMap).filter_by(mongo_id=mongo_res[-1]['id']).count() > 0:
                print('Skipping first ', skip+max_limit)
                continue

        # load mapped ids in memory
        mongo_res = map_ids(sql_storage, ['initial_molecule', 'final_molecule'], MoleculeMap, mongo_res)
        mongo_res = map_ids(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)

        # trajectory
        ids_map = []
        for res in mongo_res:
            if 'trajectory' in res and res['trajectory']:
                ids_map.extend(res['trajectory'])

        with sql_storage.session_scope() as session:
            objs = session.query(ResultMap).filter(ResultMap.mongo_id.in_(ids_map)).all()
            ids_map = {i.mongo_id:i.sql_id for i in objs}

        # replace mongo ids Results with sql
        for res in mongo_res:
            if 'trajectory' in res and res['trajectory']:
                res['trajectory'] = [ids_map[i] for i in res['trajectory']]

        results_py = [OptimizationRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_procedures(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        with sql_storage.session_scope() as session:
            obj_map = []
            for mongo_obj, sql_id in zip(mongo_res, sql_insered):
                obj_map.append(OptimizationMap(sql_id=sql_id, mongo_id=mongo_obj['id']))

            session.add_all(obj_map)
            session.commit()

        if with_check:
            with sql_storage.session_scope() as session:
                proc = session.query(OptimizationMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

            ret = sql_storage.get_procedures(id=[proc])
            print('Get from SQL:', ret['data'])

            ret2 = mongo_storage.get_procedures(id=[mongo_res[0]['id']])
            print('Get from Mongo:', ret2['data'])
            assert ret2['data'][0]['hash_index'] == ret['data'][0]['hash_index']

def main():

    global sql_uri, mongo_uri, mongo_db_name, MAX_LIMIT

    parser = argparse.ArgumentParser(description='Migrate QCFractal from Mongo to SQL DB')

    parser.add_argument('--clear-sql', type=bool, default=False,
                        help='Clear the SQL DB before importing the Mongo data')
    parser.add_argument('--check-db', type=bool, default=False,
                        help='Clear the SQL DB before importing the Mongo data')
    parser.add_argument('--sql_uri', type=str, default=sql_uri, help='SQL DB URI')
    parser.add_argument('--mongo_uri', type=str, default=mongo_uri, help='Mongo DB URI')
    parser.add_argument('--mongo_db_name', type=str, default=mongo_db_name, help='Mongo DB name')
    parser.add_argument('--max-limit', type=str, default=MAX_LIMIT,
                        help='Max number of records to read or store at a time')

    args = vars(parser.parse_args())
    print('Running migration with args: ', args)

    mongo_storage, sql_storage = connect_to_DBs(args['mongo_uri'], args['sql_uri'],
                                                args['mongo_db_name'], args['max_limit'])

    if args['clear_sql']:
        sql_storage._clear_db('')

    copy_molecules(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    copy_keywords(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    copy_kv_store(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    copy_results(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    copy_optimization_procedure(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])

if __name__ == "__main__":
    main()