"""
A command line script to migrate the mongo DB in version 0.6.0 to the
SQL DB in version 0.7.0

"""

import argparse
from qcfractal.storage_sockets import storage_socket_factory
from qcfractal.storage_sockets.sql_models import (MoleculeORM, KeywordsORM, KVStoreORM, ResultORM,
                                                 OptimizationProcedureORM, GridOptimizationProcedureORM,
                                                 TorsionDriveProcedureORM, TaskQueueORM, QueueManagerORM, UserORM)
from qcfractal.interface.models import (KeywordSet, ResultRecord, OptimizationRecord,
                                        TorsionDriveRecord, GridOptimizationRecord)


production_uri = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/test_qcarchivedb"
# production_uri = "postgresql+psycopg2://postgres@localhost:5432/qcarchivedb"
staging_uri = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/staging_qcarchivedb"
SAMPLE_SIZE = 0.1  # 10%
MAX_LIMIT = 10000


def connect_to_DBs(staging_uri, production_uri, max_limit):

    staging_storage = storage_socket_factory(staging_uri, 'qcarchivedb_staging', db_type='sqlalchemy',
                                         max_limit=max_limit)

    production_storage = storage_socket_factory(production_uri, 'qcarchivedb', db_type='sqlalchemy',
                                         max_limit=max_limit)

    print("DB limit: ", max_limit)

    return staging_storage, production_storage


def copy_molecules(staging_storage, prod_storage, prod_ids):
    """Copy from production to staging"""

    prod_ids = list(set(prod_ids))
    print('----Total # of Molecules to copy: ', len(prod_ids))

    ret = prod_storage.get_molecules(id=prod_ids)
    print('Get from prod:', ret)
    staging_ids = staging_storage.add_molecules(ret['data'])
    print('Add to staging:', staging_ids)

    map = {m1: m2 for m1, m2 in zip(prod_ids, staging_ids['data'])}
    print('MAP: ', map)

    print('---- Done copying molecules\n\n')

    return map


def copy_keywords(staging_storage, prod_storage, prod_ids):
    """Copy from production to staging"""

    prod_ids = list(set(prod_ids))
    print('----Total # of keywords to copy: ', len(prod_ids))


    ret = prod_storage.get_keywords(id=prod_ids)
    print('Get from prod:', ret)
    staging_ids = staging_storage.add_keywords(ret['data'])
    print('Add to staging:', staging_ids)

    map = {m1: m2 for m1, m2 in zip(prod_ids, staging_ids['data'])}
    print('MAP: ', map)

    print('---- Done copying keywords\n\n')

    return map


def copy_kv_store(staging_storage, prod_storage, prod_ids):
    """Copy from production to staging"""

    prod_ids = list(set(prod_ids))
    print('----Total # of KV_store to copy: ', len(prod_ids))


    ret = prod_storage.get_kvstore(id=prod_ids)
    print('Get from prod:', ret)
    staging_ids = staging_storage.add_kvstore(ret['data'].values())
    print('Add to staging:', staging_ids)

    map = {m1: m2 for m1, m2 in zip(prod_ids, staging_ids['data'])}
    print('MAP: ', map)

    print('---- Done copying KV_store \n\n')

    return map


def copy_users(staging_storage, prod_storage):
    """Copy all users from production to staging"""

    prod_users = prod_storage._get_users()
    print('-----Total # of Users in the DB is: ', len(prod_users))


    sql_insered = staging_storage._copy_users(prod_users)['data']
    print('Inserted in SQL:', len(sql_insered))


    print('---- Done copying Users\n\n')


def copy_managers(staging_storage, prod_storage):
    """Copy ALL managers from prod to staging"""

    prod_mangers = prod_storage.get_managers()
    print('-----Total # of Managers in the DB is: ', prod_mangers['meta']['n_found'])


    sql_insered = staging_storage._copy_managers(prod_mangers['data'])['data']
    print('Inserted in SQL:', len(sql_insered))

    print('---- Done copying Queue Manager\n\n')


def copy_results(staging_storage, production_storage, SAMPLE_SIZE=0, results_ids=[]):
    """Copy from mongo to sql"""

    results_ids = list(set(results_ids))
    total_count = production_storage.get_total_count(ResultORM)
    print('------Total # of Results in the DB is: ', total_count)
    if results_ids:
        count_to_copy = len(results_ids)
        prod_results = production_storage.get_results(id=results_ids, status=None)['data']
    else:
        count_to_copy = int(total_count*SAMPLE_SIZE)
        prod_results = production_storage.get_results(status=None, limit=count_to_copy)['data']

    print('Copying {} results'.format(count_to_copy))

    mols, keywords, kvstore = [], [], []
    for result in prod_results:
        mols.append(result['molecule'])
        if result['keywords']:
            keywords.append(result['keywords'])
        if result['stdout']:
            kvstore.append(result['stdout'])
        if result['stderr']:
            kvstore.append(result['stderr'])
        if result['error']:
            kvstore.append(result['error'])

    mols_map = copy_molecules(staging_storage, production_storage, mols)
    keywords_map = copy_keywords(staging_storage, production_storage, keywords)
    kvstore_map = copy_kv_store(staging_storage, production_storage, kvstore)

    for result in prod_results:
        result['molecule'] = mols_map[result['molecule']]
        if result['keywords']:
            result['keywords'] = keywords_map[result['keywords']]
        if result['stdout']:
            result['stdout'] = kvstore_map[result['stdout']]
        if result['stderr']:
            result['stderr'] = kvstore_map[result['stderr']]
        if result['error']:
            result['error'] = kvstore_map[result['error']]

    results_py = [ResultRecord(**res) for res in prod_results]
    staging_ids = staging_storage.add_results(results_py)['data']
    print('Inserted in SQL:', len(staging_ids))

    print('---- Done copying Results\n\n')

    return {m1: m2 for m1, m2 in zip(results_ids, staging_ids)}


def copy_optimization_procedure(staging_storage, production_storage, SAMPLE_SIZE=None,
                                procedure_ids=None):
    """Copy from prod to staging"""


    total_count = production_storage.get_total_count(OptimizationProcedureORM)
    print('------Total # of Optmization Procedure in the DB is: ', total_count)

    if procedure_ids:
        count_to_copy = len(procedure_ids)
        prod_proc = production_storage.get_procedures(id=procedure_ids, procedure='optimization', status=None)['data']
    else:
        count_to_copy = int(total_count*SAMPLE_SIZE)
        prod_proc = production_storage.get_procedures(procedure='optimization', status=None, limit=count_to_copy)['data']

    print('Copying {} optimizations'.format(count_to_copy))


    mols, results, kvstore = [], [], []
    for rec in prod_proc:
        mols.append(rec['initial_molecule'])
        if rec['final_molecule']:
            mols.append(rec['final_molecule'])
        if rec['trajectory']:
            print('traj: ', rec['trajectory'])
            results.extend(rec['trajectory'])
        if rec['stdout']:
            kvstore.append(rec['stdout'])
        if rec['stderr']:
            kvstore.append(rec['stderr'])
        if rec['error']:
            kvstore.append(rec['error'])

    mols_map = copy_molecules(staging_storage, production_storage, mols)
    results_map = copy_results(staging_storage, production_storage, results_ids=results)
    kvstore_map = copy_kv_store(staging_storage, production_storage, kvstore)

    print(mols_map)
    print(results_map)

    for rec in prod_proc:
        rec['initial_molecule'] = mols_map[rec['initial_molecule']]
        if rec['final_molecule']:
            rec['final_molecule'] = mols_map[rec['final_molecule']]
        if rec['trajectory']:
            rec['trajectory'] = [results_map[i] for i in rec['trajectory']]
        if rec['stdout']:
            rec['stdout'] = kvstore_map[rec['stdout']]
        if rec['stderr']:
            rec['stderr'] = kvstore_map[rec['stderr']]
        if rec['error']:
            rec['error'] = kvstore_map[rec['error']]

    procedures_py = [OptimizationRecord(**proc) for proc in prod_proc]
    sql_insered = staging_storage.add_procedures(procedures_py)['data']
    print('Inserted in SQL:', len(sql_insered))

    print('---- Done copying Optimization procedures\n\n')


def copy_torsiondrive_procedure(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ProcedureORM, procedure='torsiondrive')
    print('-----Total # of Torsiondrive in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_procedures(procedure='torsiondrive', status=None, limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, ProcedureMap, mongo_res[-1]['id']):
            print('Skipping first ', skip+max_limit)
            continue

        # load mapped ids in memory
        mongo_res = get_ids_map(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['initial_molecule'], MoleculeMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['optimization_history'], ProcedureMap, mongo_res)

        results_py = [TorsionDriveRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_procedures(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        mongo_ids = [obj['id'] for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, ProcedureMap)

        if with_check:
            with sql_storage.session_scope() as session:
                proc = session.query(ProcedureMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

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


def copy_grid_optimization_procedure(sta, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(ProcedureORM, procedure='gridoptimization')
    print('-----Total # of Grid Optmizations in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_procedures(procedure='gridoptimization', status=None, limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, ProcedureMap, mongo_res[-1]['id']):
            print('Skipping first ', skip+max_limit)
            continue

        # load mapped ids in memory
        mongo_res = get_ids_map(sql_storage, ['stdout', 'stderr', 'error'], KVStoreMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['initial_molecule', 'starting_molecule'], MoleculeMap, mongo_res)
        mongo_res = get_ids_map(sql_storage, ['grid_optimizations'], ProcedureMap, mongo_res)

        results_py = [GridOptimizationRecord(**res) for res in mongo_res]
        sql_insered = sql_storage.add_procedures(results_py)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        mongo_ids = [obj['id'] for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, ProcedureMap)

        if with_check:
            with sql_storage.session_scope() as session:
                proc = session.query(ProcedureMap).filter_by(mongo_id=mongo_res[0]['id']).first().sql_id

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


def copy_task_queue(mongo_storage, sql_storage, max_limit, with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(TaskQueueORM)
    print('-----Total # of Tasks in the DB is: ', total_count)

    for skip in range(0, total_count, max_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_queue(limit=max_limit, skip=skip)
        mongo_res= ret['data']
        print('mongo results returned: ', len(mongo_res), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        if is_mapped(sql_storage, TaskQueueMap, mongo_res[-1].id):
            print('Skipping first ', skip+max_limit)
            continue

        results_ids_map, procedures_ids_map = [], []
        for res in mongo_res:
            if res.base_result.ref == 'result':
                results_ids_map.append(res.base_result.id)
            else:
                procedures_ids_map.append(res.base_result.id)

        with sql_storage.session_scope() as session:
            objs = session.query(ResultMap).filter(ResultMap.mongo_id.in_(set(results_ids_map))).all()
            assert len(objs) == len(set(results_ids_map))
            results_ids_map = {i.mongo_id:i.sql_id for i in objs}

            objs = session.query(ProcedureMap).filter(ProcedureMap.mongo_id.in_(set(procedures_ids_map))).all()
            assert len(objs) == len(set(procedures_ids_map))
            procedures_ids_map = {i.mongo_id:i.sql_id for i in objs}

        # replace mongo ids Results with sql
        for res in mongo_res:
            if res.base_result.ref == 'result':
                res.base_result.id = results_ids_map[res.base_result.id]
            else:
                res.base_result.id = procedures_ids_map[res.base_result.id]

        sql_insered = sql_storage._copy_task_to_queue(mongo_res)['data']
        print('Inserted in SQL:', len(sql_insered))

        # store the ids mapping in the sql DB
        mongo_ids = [obj.id for obj in mongo_res]
        store_ids_map(sql_storage, mongo_ids, sql_insered, TaskQueueMap)

        if with_check:
            with sql_storage.session_scope() as session:
                mongo_task = mongo_storage.get_queue(id=[mongo_res[0].id])['data'][0]

                task_sql_id = session.query(TaskQueueMap).filter_by(mongo_id=mongo_res[0].id).first().sql_id
                sql_task = sql_storage.get_queue(id=[task_sql_id])['data'][0]
                print('Get from SQL:', sql_task)

                mongo_base_result_id = mongo_task.base_result.id
                className = ResultMap if mongo_task.base_result.ref == 'result' else ProcedureMap
                sql_base_result_id_mapped = session.query(className.sql_id).filter_by(mongo_id=mongo_base_result_id).first()[0]

                assert str(sql_base_result_id_mapped) == sql_task.base_result.id

    print('---- Done copying Task Queue\n\n')


def main():

    global staging_uri, production_uri, SAMPLE_SIZE, MAX_LIMIT

    staging_storage, production_storage = connect_to_DBs(staging_uri, production_uri, MAX_LIMIT)

    del_staging = input("Are you sure you want to delete the DB in the URI:\n{}? yes or no\n".format(staging_uri))
    if del_staging.lower() == 'yes':
        staging_storage._clear_db('')
    else:
        print('Exit without creating the DB.')
        return

    # copy all managers and users, small tables, no need for sampling
    copy_users(staging_storage, production_storage)
    copy_managers(staging_storage, production_storage)

    # copy sample of results and procedures
    print('\n-------------- Results -----------------')
    copy_results(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE)

    print('\n---------------- Optimization Procedures -------------------')
    copy_optimization_procedure(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE*2)

    # copy_torsiondrive_procedure(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    # copy_grid_optimization_procedure(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    # copy_task_queue(mongo_storage, sql_storage, args['max_limit'], with_check=args['check_db'])
    #

if __name__ == "__main__":
    main()