import argparse
from qcfractal.storage_sockets import storage_socket_factory
from qcfractal.storage_sockets.sql_models import (ProcedureMap, OptimizationProcedureORM, OptimizationHistory,
                                                  TorsionDriveProcedureORM, GridOptimizationProcedureORM,
                                                  GridOptimizationAssociation, Trajectory, torsion_init_mol_association)


sql_uri = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/qcarchivedb"


sql_storage = storage_socket_factory(sql_uri, 'qcarchivedb', db_type='sqlalchemy')

with sql_storage.engine.connect() as con:

    con.execute('ALTER TABLE optimization_history ' +
                    'DROP CONSTRAINT optimization_history_pkey;')

    con.execute('ALTER TABLE optimization_history '
                    'ADD CONSTRAINT optimization_history_pkey '
                    'PRIMARY KEY (torsion_id, opt_id, key, position);')

# with sql_storage.session_scope() as session:

        # procedures = session.query(OptimizationProcedureORM).all()
        # print('Deleteing Opt proc: ', len(procedures))
        # # delete through session to delete correctly from base_result
        # for proc in procedures:
        #     session.delete(proc)
        #
        # procedures = session.query(TorsionDriveProcedureORM).all()
        # print('Deleteing Torsion proc: ', len(procedures))
        # # delete through session to delete correctly from base_result
        # for proc in procedures:
        #     session.delete(proc)
        #
        # procedures = session.query(GridOptimizationProcedureORM).all()
        # print('Deleteing Grid proc: ', len(procedures))
        # # delete through session to delete correctly from base_result
        # for proc in procedures:
        #     session.delete(proc)
        #
        # session.commit()
        #
        # # drop tables
        # torsion_init_mol_association.drop(sql_storage.engine)
        # OptimizationHistory.__table__.drop(sql_storage.engine)
        # GridOptimizationAssociation.__table__.drop(sql_storage.engine)
        # Trajectory.__table__.drop(sql_storage.engine)
        #
        # OptimizationProcedureORM.__table__.drop(sql_storage.engine)
        # TorsionDriveProcedureORM.__table__.drop(sql_storage.engine)
        # GridOptimizationProcedureORM.__table__.drop(sql_storage.engine)
        #
        # ProcedureMap.__table__.drop(sql_storage.engine)




