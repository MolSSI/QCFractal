from qcfractal.storage_sockets import storage_socket_factory
from qcfractal.storage_sockets.me_models import MoleculeORM
from qcfractal.storage_sockets.sql_models import MoleculeMap


sql_uri = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/qcarchivedb"
mongo_uri = "mongodb://localhost:27017"
mongo_db_name = "qcf_compute_server_test"

MAX_LIMIT = 100
mongo_storage = storage_socket_factory(mongo_uri, mongo_db_name, db_type="mongoengine",
                                       max_limit=MAX_LIMIT)

sql_storage = storage_socket_factory(sql_uri, 'qcarchivedb', db_type='sqlalchemy',
                                     max_limit=MAX_LIMIT)

m_limit = mongo_storage.get_limit(MAX_LIMIT)
print("mongo limit: ", m_limit)

s_limit = sql_storage.get_limit(MAX_LIMIT)  #_max_limit
print("sql limit: ", s_limit)


def copy_molecules(with_check=False):
    """Copy from mongo to sql"""

    total_count = mongo_storage.get_total_count(MoleculeORM)
    print('Total # of Molecules in the DB is: ', total_count)

    for skip in range(0, total_count, m_limit):

        print('\nCurrent skip={}\n-----------'.format(skip))
        ret = mongo_storage.get_molecules(limit=m_limit, skip=skip)
        mongo_mols = ret['data']
        print('mongo mol returned: ', len(mongo_mols), ', total: ', ret['meta']['n_found'])

        # check if this patch has been already stored
        with sql_storage.session_scope() as session:
            if session.query(MoleculeMap).filter_by(mongo_id=mongo_mols[-1].id).count() > 0:
                print('Skipping first ', skip+m_limit)
                continue

        sql_insered = sql_storage.add_molecules(mongo_mols)['data']
        print('Inserted in SQL:', len(sql_insered))

        if with_check:
            ret = sql_storage.get_molecules(limit=m_limit, skip=skip)
            print('Get from SQL: n_found={}, returned={}'.format(ret['meta']['n_found'], len(ret['data'])))

            assert mongo_mols[0].compare(ret['data'][0])

        # store the ids mapping in the sql DB
        with sql_storage.session_scope() as session:
            mol_map = []
            for mongo_obj, sql_id in zip(mongo_mols, sql_insered):
                mol_map.append(MoleculeMap(sql_id=sql_id, mongo_id=mongo_obj.id))

            session.add_all(mol_map)
            session.commit()


if __name__ == "__main__":

    sql_storage._clear_db('qcarchivedb')
    copy_molecules(with_check=True)