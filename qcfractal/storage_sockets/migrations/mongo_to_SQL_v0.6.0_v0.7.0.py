"""
A command line script to migrate the mongo DB in version 0.6.0 to the
SQL DB in version 0.7.0

"""

import argparse
from qcfractal.storage_sockets import storage_socket_factory
from qcfractal.storage_sockets.me_models import (MoleculeORM, KeywordsORM, KVStoreORM, ResultORM,
                                                 ProcedureORM)
from qcfractal.storage_sockets.sql_models import (MoleculeMap, KeywordsMap, KVStoreMap, ResultMap,
                                                  OptimizationMap, TorsiondriveMap, GridOptizationMap)
from qcfractal.interface.models import (KeywordSet, ResultRecord, OptimizationRecord,
                                        TorsionDriveRecord, GridOptimizationRecord)


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


def get_ids_map(sql_storage, field_names, mappingClass, mongo_res):
    # load mapped ids in memory
    ids_map = []
    for res in mongo_res:
        for field in field_names:
            if field in res and res[field]:
                if isinstance(res[field], (list, tuple)):
                    ids_map.extend(res[field])
                elif isinstance(res[field], dict):
                    if isinstance(list(res[field].values())[0], list):
                        ids_map.extend(set().union(*res[field].values()))
                    else:
                        ids_map.extend([val for val in res[field].values()])
                else:
                    ids_map.append(res[field])

    with sql_storage.session_scope() as session:
        objs = session.query(mappingClass).filter(mappingClass.mongo_id.in_(set(ids_map))).all()
        ids_map = {i.mongo_id:i.sql_id for i in objs}

    # replace mongo ids Results with sql
    for res in mongo_res:
        for field in field_names:
            if field in res and res[field]:
                if isinstance(res[field], (list, tuple)):
                    res[field] = [ids_map[i] for i in res[field]]
                elif isinstance(res[field], dict):
                    for key, values in res[field].items():
                        if isinstance(values, list):
                            res[field][key] = [ids_map[val] for val in values]
                        else:
                            res[field][key] = ids_map[values]
                else:
                    res[field] = ids_map[res[field]]

    return mongo_res


def store_ids_map(sql_storage, mongo_ids, sql_ids, mappingClass):

    with sql_storage.session_scope() as session:
        obj_map = []
        for mongo_id, sql_id in zip(mongo_ids, sql_ids):
            obj_map.append(mappingClass(sql_id=sql_id, mongo_id=mongo_id))

        session.add_all(obj_map)
        session.commit()

def is_mapped(sql_storage, mappingClass, monog_id):
    # check if this id has been already stored
        with sql_storage.session_scope() as session:
            if session.query(mappingClass).filter_by(mongo_id=monog_id).count() > 0:
                return True
        return False


def copy_molecules(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(MoleculeORM)
    print('----Total # of Molecules in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_molecules(limit=max_limit, skip=skip)
        mongo_res = ret['data']
        print('mongo mol returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, MoleculeMap, mongo_res[-1].id):
            print('Skipping first ', skip+max_limit)
            continue

        sql_insered = sql_storage.add_molecules(mongo_res)['data']
        print('Inserted in SQL:', len(sql_insered))

        if with_check:
            ret = sql_storage.get_molecules(limit=max_limit, skip=skip)
            print('Get from SQL: n_found={}, returned={}'.format(ret['meta']['n_found'], len(ret['data'])))

            assert mongo_res[0].compare(ret['data'][0])

        # store the ids mapping in the sql DB
        mongo_ids = [obj.id for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, MoleculeMap)

    print('---- Done copying molecules\n\n')


def copy_keywords(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    mongo_storage.add_keywords([KeywordSet(values={'key': 'test data'})])

    total_count = mongo_storage.get_total_count(KeywordsORM)
    print('-----Total # of Keywords in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_keywords(limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        if is_mapped(sql_storage, KeywordsMap, mongo_res[-1].id):
            print('Skipping first ', skip+max_limit)
            continue

        sql_insered = sql_storage.add_keywords(mongo_res)['data']
        print('Inserted in SQL:', len(sql_insered))

        if with_check:
            ret = sql_storage.get_keywords(limit=max_limit, skip=skip)
            print('Get from SQL: n_found={}, returned={}'.format(ret['meta']['n_found'], len(ret['data'])))

            assert mongo_res[0].hash_index == ret['data'][0].hash_index

        # store the ids mapping in the sql DB
        mongo_ids = [obj.id for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, KeywordsMap)

    print('---- Done copying keywords\n\n')


def copy_kv_store(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(KVStoreORM)
    print('------Total # of KV_store in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_kvstore(limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo kv_store returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        ids = list(mongo_res.keys())
        values = mongo_res.values()
        # check if this patch has been already stored
        if is_mapped(sql_storage, KVStoreMap, ids[-1]):
            print('Skipping first ', skip+max_limit)
            continue

        sql_insered = sql_storage.add_kvstore(values)['data']
        print('Inserted in SQL:', len(sql_insered))

        if with_check:
            ret = sql_storage.get_kvstore(limit=max_limit, skip=skip)
            print('Get from SQL: n_found={}, returned={}'.format(ret['meta']['n_found'], len(ret['data'])))

            assert list(values)[0] == list(ret['data'].values())[0]

        # store the ids mapping in the sql DB
        store_ids_map(sql_storage, ids, sql_insered, KVStoreMap)

    print('---- Done copying KV_store\n\n')


def copy_results(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ResultORM)
    print('------Total # of Results in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_results(limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, ResultMap, mongo_res[-1]['id']):
            print('Skipping first ', skip+max_limit)
            continue

        # load mapped ids in memory
        mongo_res = get_ids_map(sql_storage, ['molecule'], MoleculeMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['keywords'], KeywordsMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)

        results_py = [ResultRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_results(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        mongo_ids = [obj['id'] for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, ResultMap)

        if with_check:
            with sql_storage.session_scope() as session:
                res = session.query(ResultMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

            ret = sql_storage.get_results(id=[res])
            print('Get from SQL:', ret['data'])

            ret2 = mongo_storage.get_results(id=[mongo_res[0]['id']])
            print('Get from Mongo:', ret2['data'])
            assert ret2['data'][0]['return_result'] == ret['data'][0]['return_result']

    print('---- Done copying Results\n\n')


def copy_optimization_procedure(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ProcedureORM, procedure='optimization')
    print('------Total # of Optimization in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_procedures(procedure='optimization', limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, OptimizationMap, mongo_res[-1]['id']):
            print('Skipping first ', skip+max_limit)
            continue

        # load mapped ids in memory
        mongo_res = get_ids_map(sql_storage, ['initial_molecule', 'final_molecule'], MoleculeMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['trajectory'], ResultMap, mongo_res)

        results_py = [OptimizationRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_procedures(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        mongo_ids = [obj['id'] for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, OptimizationMap)

        if with_check:
            with sql_storage.session_scope() as session:
                proc = session.query(OptimizationMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

            ret = sql_storage.get_procedures(id=[proc])
            print('Get from SQL:', ret['data'])

            ret2 = mongo_storage.get_procedures(id=[mongo_res[0]['id']])
            print('Get from Mongo:', ret2['data'])
            assert ret2['data'][0]['hash_index'] == ret['data'][0]['hash_index']

    print('---- Done copying Optimization procedures\n\n')


def copy_torsiondrive_procedure(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ProcedureORM, procedure='torsiondrive')
    print('-----Total # of Torsiondrive in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_procedures(procedure='torsiondrive', limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, TorsiondriveMap, mongo_res[-1]['id']):
            print('Skipping first ', skip+max_limit)
            continue

        # load mapped ids in memory
        mongo_res = get_ids_map(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['initial_molecule'], MoleculeMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['optimization_history'], OptimizationMap, mongo_res)

        results_py = [TorsionDriveRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_procedures(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        mongo_ids = [obj['id'] for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, TorsiondriveMap)

        if with_check:
            with sql_storage.session_scope() as session:
                proc = session.query(TorsiondriveMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

            ret = sql_storage.get_procedures(id=[proc])
            print('Get from SQL:', ret['data'])

            ret2 = mongo_storage.get_procedures(id=[mongo_res[0]['id']])
            print('Get from Mongo:', ret2['data'])
            assert ret2['data'][0]['hash_index'] == ret['data'][0]['hash_index']
            assert ret2['data'][0]['optimization_history'].keys() == \
                                   ret['data'][0]['optimization_history'].keys()
            print(ret2['data'][0]['optimization_history'])
            print(ret['data'][0]['optimization_history'])

    print('---- Done copying Torsiondrive procedures\n\n')


def copy_grid_optimization_procedure(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ProcedureORM, procedure='gridoptimization')
    print('-----Total # of Grid Optmizations in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_procedures(procedure='gridoptimization', limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, GridOptizationMap, mongo_res[-1]['id']):
            print('Skipping first ', skip+max_limit)
            continue

        # load mapped ids in memory
        mongo_res = get_ids_map(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['initial_molecule', 'starting_molecule'], MoleculeMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['grid_optimizations'], OptimizationMap, mongo_res)

        results_py = [GridOptimizationRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_procedures(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        mongo_ids = [obj['id'] for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, GridOptizationMap)

        if with_check:
            with sql_storage.session_scope() as session:
                proc = session.query(GridOptizationMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

            ret = sql_storage.get_procedures(id=[proc])
            print('Get from SQL:', ret['data'])

            ret2 = mongo_storage.get_procedures(id=[mongo_res[0]['id']])
            print('Get from Mongo:', ret2['data'])
            assert ret2['data'][0]['hash_index'] == ret['data'][0]['hash_index']
            assert ret2['data'][0]['grid_optimizations'].keys() == \
                                   ret['data'][0]['grid_optimizations'].keys()
            print(ret2['data'][0]['grid_optimizations'])
            print(ret['data'][0]['grid_optimizations'])


    print('---- Done copying Grid Optmization procedures\n\n')


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
    copy_torsiondrive_procedure(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    copy_grid_optimization_procedure(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])

if __name__ == "__main__":
    main()