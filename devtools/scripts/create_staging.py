"""
A command line script to copy a sample of the production DB into staging or
development DB.

"""

from qcfractal.storage_sockets import storage_socket_factory
from qcfractal.storage_sockets.models import (BaseResultORM, ResultORM, CollectionORM,
                                              OptimizationProcedureORM, GridOptimizationProcedureORM,
                                              TorsionDriveProcedureORM, TaskQueueORM)
from qcfractal.interface.models import (ResultRecord, OptimizationRecord,
                                        TorsionDriveRecord, GridOptimizationRecord)

# production_uri = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/test_qcarchivedb"
production_uri = "postgresql+psycopg2://postgres:@localhost:11711/qcarchivedb"
staging_uri = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/staging_qcarchivedb"
SAMPLE_SIZE = 0.0001  # 0.1 is 10%
MAX_LIMIT = 10000


def connect_to_DBs(staging_uri, production_uri, max_limit):

    staging_storage = storage_socket_factory(staging_uri, db_type='sqlalchemy',
                                         max_limit=max_limit)

    production_storage = storage_socket_factory(production_uri, db_type='sqlalchemy',
                                         max_limit=max_limit)

    print("DB limit: ", max_limit)

    return staging_storage, production_storage


def get_number_to_copy(total_size, sample_size):
    to_copy = int(total_size*sample_size)
    if to_copy:
        return to_copy
    else:
        return 1  # avoid zero because zero means no limit in storage

def copy_molecules(staging_storage, prod_storage, prod_ids):
    """Copy from production to staging"""

    if len(prod_ids) == 0:
        return

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

    if len(prod_ids) == 0:
        return

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

    if len(prod_ids) == 0:
        return

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


def copy_managers(staging_storage, prod_storage, mang_list):
    """Copy ALL managers from prod to staging"""

    prod_mangers = []
    for mang in mang_list:
        prod_mangers.extend(prod_storage.get_managers(name=mang)['data'])

    print('-----Total # of Managers to copy is: ', len(prod_mangers))


    sql_insered = staging_storage._copy_managers(prod_mangers)['data']
    print('Inserted in SQL:', len(sql_insered))

    print('---- Done copying Queue Manager\n\n')


def copy_collections(staging_storage, production_storage, SAMPLE_SIZE=0):
    """Copy collections from production to staging"""

    total_count = production_storage.get_total_count(CollectionORM)
    print('------Total # of Collections in the DB is: ', total_count)
    count_to_copy = get_number_to_copy(total_count, SAMPLE_SIZE)
    prod_results = production_storage.get_collections(limit=count_to_copy)['data']

    print('Copying {} collections'.format(count_to_copy))

    sql_insered = 0
    for col in prod_results:
        ret = staging_storage.add_collection(col)['data']
        sql_insered += 1
    print('Inserted in SQL:', sql_insered)

    print('---- Done copying Collections\n\n')


def copy_results(staging_storage, production_storage, SAMPLE_SIZE=0, results_ids=[]):
    """Copy from mongo to sql"""

    if SAMPLE_SIZE == 0 and len(results_ids) == 0:
        return []

    results_ids = list(set(results_ids))

    if results_ids:
        count_to_copy = len(results_ids)
        prod_results = production_storage.get_results(id=results_ids, status=None)['data']
    else:
        total_count = production_storage.get_total_count(ResultORM)
        print('------Total # of Results in the DB is: ', total_count)
        count_to_copy = get_number_to_copy(total_count, SAMPLE_SIZE)
        prod_results = production_storage.get_results(status=None, limit=count_to_copy)['data']


    print('Copying {} results'.format(count_to_copy))

    mols, keywords, kvstore = [], [], []
    for result in prod_results:
        if result['molecule']:
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


def copy_optimization_procedure(staging_storage, production_storage, SAMPLE_SIZE=0,
                                procedure_ids=[]):
    """Copy from prod to staging"""

    if SAMPLE_SIZE == 0 and len(procedure_ids) == 0:
        return []

    if procedure_ids:
        count_to_copy = len(procedure_ids)
        prod_proc = production_storage.get_procedures(id=procedure_ids, procedure='optimization', status=None)['data']
    else:
        total_count = production_storage.get_total_count(OptimizationProcedureORM)
        print('------Total # of Optmization Procedure in the DB is: ', total_count)
        count_to_copy = get_number_to_copy(total_count, SAMPLE_SIZE)
        prod_proc = production_storage.get_procedures(procedure='optimization', status=None, limit=count_to_copy)['data']

    print('Copying {} optimizations'.format(count_to_copy))


    mols, results, kvstore = [], [], []
    for rec in prod_proc:
        if rec['initial_molecule']:
            mols.append(rec['initial_molecule'])
        if rec['final_molecule']:
            mols.append(rec['final_molecule'])
        if rec['trajectory']:
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

    for rec in prod_proc:
        if rec['initial_molecule']:
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
    staging_ids = staging_storage.add_procedures(procedures_py)['data']
    print('Inserted in SQL:', len(staging_ids))

    print('---- Done copying Optimization procedures\n\n')

    return {m1: m2 for m1, m2 in zip(procedure_ids, staging_ids)}


def copy_torsiondrive_procedure(staging_storage, production_storage, SAMPLE_SIZE=0,
                                procedure_ids=[]):
    """Copy from prod to staging"""

    if SAMPLE_SIZE == 0 and len(procedure_ids) == 0:
        return []

    if procedure_ids:
        count_to_copy = len(procedure_ids)
        prod_proc = production_storage.get_procedures(id=procedure_ids, procedure='torsiondrive', status=None)['data']
    else:
        total_count = production_storage.get_total_count(TorsionDriveProcedureORM)
        print('------Total # of Torsiondrive Procedure in the DB is: ', total_count)
        count_to_copy = get_number_to_copy(total_count, SAMPLE_SIZE)
        prod_proc = production_storage.get_procedures(procedure='torsiondrive', status=None, limit=count_to_copy)['data']

    print('Copying {} Torsiondrives'.format(count_to_copy))


    mols, procs, kvstore = [], [], []
    for rec in prod_proc:
        if rec['initial_molecule']:
            mols.extend(rec['initial_molecule'])
        if rec['optimization_history']:
            for i in rec['optimization_history'].values():
                procs.extend(i)
        if rec['stdout']:
            kvstore.append(rec['stdout'])
        if rec['stderr']:
            kvstore.append(rec['stderr'])
        if rec['error']:
            kvstore.append(rec['error'])

    mols_map = copy_molecules(staging_storage, production_storage, mols)
    proc_map = copy_optimization_procedure(staging_storage, production_storage, procedure_ids=procs)
    kvstore_map = copy_kv_store(staging_storage, production_storage, kvstore)

    for rec in prod_proc:

        if rec['initial_molecule']:
            rec['initial_molecule'] = [mols_map[i] for i in rec['initial_molecule']]
        if rec['optimization_history']:
            for key, proc_list in rec['optimization_history'].items():
                rec['optimization_history'][key] = [proc_map[i] for i in proc_list]
        if rec['stdout']:
            rec['stdout'] = kvstore_map[rec['stdout']]
        if rec['stderr']:
            rec['stderr'] = kvstore_map[rec['stderr']]
        if rec['error']:
            rec['error'] = kvstore_map[rec['error']]

    procedures_py = [TorsionDriveRecord(**proc) for proc in prod_proc]
    staging_ids = staging_storage.add_procedures(procedures_py)['data']
    print('Inserted in SQL:', len(staging_ids))

    print('---- Done copying Torsiondrive procedures\n\n')

    return {m1: m2 for m1, m2 in zip(procedure_ids, staging_ids)}


def copy_grid_optimization_procedure(staging_storage, production_storage, SAMPLE_SIZE=0,
                                procedure_ids=[]):
    """Copy from prod to staging"""

    if SAMPLE_SIZE == 0 and len(procedure_ids) == 0:
        return []


    if procedure_ids:
        count_to_copy = len(procedure_ids)
        prod_proc = production_storage.get_procedures(id=procedure_ids, procedure='gridoptimization', status=None)['data']
    else:
        total_count = production_storage.get_total_count(GridOptimizationProcedureORM)
        print('------Total # of Grid optimizations Procedure in the DB is: ', total_count)
        count_to_copy = get_number_to_copy(total_count, SAMPLE_SIZE)
        prod_proc = production_storage.get_procedures(procedure='gridoptimization', status=None, limit=count_to_copy)['data']

    print('Copying {} Grid optimizations'.format(count_to_copy))


    mols, procs, kvstore = [], [], []
    for rec in prod_proc:
        if rec['initial_molecule']:
            mols.append(rec['initial_molecule'])
        if rec['starting_molecule']:
            mols.append(rec['starting_molecule'])
        if rec['grid_optimizations']:
            for i in rec['grid_optimizations'].values():
                procs.append(i)
        if rec['stdout']:
            kvstore.append(rec['stdout'])
        if rec['stderr']:
            kvstore.append(rec['stderr'])
        if rec['error']:
            kvstore.append(rec['error'])

    mols_map = copy_molecules(staging_storage, production_storage, mols)
    proc_map = copy_optimization_procedure(staging_storage, production_storage, procedure_ids=procs)
    kvstore_map = copy_kv_store(staging_storage, production_storage, kvstore)

    for rec in prod_proc:

        if rec['initial_molecule']:
            rec['initial_molecule'] = mols_map[rec['initial_molecule']]
        if rec['starting_molecule']:
            rec['starting_molecule'] = mols_map[rec['starting_molecule']]
        if rec['grid_optimizations']:
            for key, proc in rec['grid_optimizations'].items():
                rec['grid_optimizations'][key] = proc_map[proc]
        if rec['stdout']:
            rec['stdout'] = kvstore_map[rec['stdout']]
        if rec['stderr']:
            rec['stderr'] = kvstore_map[rec['stderr']]
        if rec['error']:
            rec['error'] = kvstore_map[rec['error']]

    procedures_py = [GridOptimizationRecord(**proc) for proc in prod_proc]
    staging_ids = staging_storage.add_procedures(procedures_py)
    print('Inserted in SQL:', len(staging_ids['data']),
          'duplicates: ', len(staging_ids['meta']['duplicates']))

    print('---- Done copying Grid Optmization procedures\n\n')

    return {m1: m2 for m1, m2 in zip(procedure_ids, staging_ids['data'])}


def copy_task_queue(staging_storage, production_storage, SAMPLE_SIZE=None):
    """Copy from prod to staging"""

    total_count = production_storage.get_total_count(TaskQueueORM)
    print('------Total # of TaskQueue in the DB is: ', total_count)

    count_to_copy = get_number_to_copy(total_count, SAMPLE_SIZE)
    prod_tasks = production_storage.get_queue(limit=count_to_copy)['data']

    print('Copying {} TaskQueues'.format(count_to_copy))


    base_results, managers = [], []
    results = {
        'result': [],
        'optimization_procedure': [],
        'torsiondrive_procedure': [],
        'grid_optimization_procedure': []
    }

    for rec in prod_tasks:
        base_results.append(rec.base_result.id)
        managers.append(rec.manager)

    with production_storage.session_scope() as session:
        ret = session.query(BaseResultORM.id, BaseResultORM.result_type).filter(BaseResultORM.id.in_(base_results)).all()

        for id, type in ret:
            results[type].append(id)


    # fast way to map, missing ids will be omitted
    results_map = copy_results(staging_storage, production_storage, results_ids=results['result'])
    proc_map1 = copy_optimization_procedure(staging_storage, production_storage, procedure_ids=results['optimization_procedure'])
    proc_map2 = copy_grid_optimization_procedure(staging_storage, production_storage, procedure_ids=results['grid_optimization_procedure'])
    proc_map3 = copy_torsiondrive_procedure(staging_storage, production_storage, procedure_ids=results['torsiondrive_procedure'])
    copy_managers(staging_storage, production_storage, managers)

    for rec in prod_tasks:
        id = int(rec.base_result.id)
        if id in results_map:
            rec.base_result.id = results_map[id]
        elif id in proc_map1:
            rec.base_result.id = proc_map1[id]
        elif id in proc_map2:
            rec.base_result.id = proc_map2[id]
        elif id in proc_map3:
            rec.base_result.id = proc_map3[id]
        else:
            raise Exception('Result not found!', rec.base_result.id)

    staging_ids = staging_storage._copy_task_to_queue(prod_tasks)['data']
    print('Inserted in SQL:', len(staging_ids))

    print('---- Done copying Task Queue\n\n')


def copy_alembic(staging_storage, production_storage):

    alembic_version = None

    with production_storage.engine.connect() as conn:
        alembic_version = conn.execute('select version_num from alembic_version;').scalar()

    print('Alembic version: ', alembic_version)

    create_table = """create table if not exists alembic_version(
    version_num varchar(32) not null
        constraint alembic_version_pkc
            primary key
    );"""

    with staging_storage.engine.connect() as conn:
        conn.execute(create_table)
        conn.execute(f"insert into alembic_version values ('{alembic_version}')")


def main():

    global staging_uri, production_uri, SAMPLE_SIZE, MAX_LIMIT

    staging_storage, production_storage = connect_to_DBs(staging_uri, production_uri, MAX_LIMIT)

    del_staging = input("Are you sure you want to delete the DB in the URI:\n{}? yes or no\n".format(staging_uri))
    if del_staging.lower() == 'yes':
        staging_storage._clear_db('')
    else:
        print('Exit without creating the DB.')
        return

    # copy all users, small tables, no need for sampling
    copy_users(staging_storage, production_storage)

    # copy sample of results and procedures
    print('\n-------------- Results -----------------')
    copy_results(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE)

    print('\n---------------- Optimization Procedures -------------------')
    copy_optimization_procedure(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE)

    print('\n---------------- Torsiondrive procedure -----------------------')
    copy_torsiondrive_procedure(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE*2)

    print('\n---------------- Grid Optimization procedure -----------------------')
    copy_grid_optimization_procedure(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE*2)

    print('\n---------------- Task Queue -----------------------')
    copy_task_queue(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE*2)

    print('\n---------------- Collections -----------------------')
    copy_collections(staging_storage, production_storage, SAMPLE_SIZE=SAMPLE_SIZE*2)

    print('\n---------------- Alembic -----------------------')
    copy_alembic(staging_storage, production_storage)


if __name__ == "__main__":
    main()