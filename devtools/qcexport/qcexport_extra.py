'''
Handles special relationships
'''

##########################################
# TODO - fix import mess
# Cannot import _general_copy here since it
# will result in a circular import
##########################################


from qcexport_extra_collection import _add_collection

from qcfractal.components.collections.db_models import CollectionORM
from qcfractal.components.records.torsiondrive.db_models import TorsionDriveProcedureORM
from qcfractal.components.records.gridoptimization.db_models import GridOptimizationProcedureORM
from qcfractal.components.records.optimization.db_models import OptimizationProcedureORM
from qcfractal.components.managers.db_models import QueueManagerORM


def _add_procedure_mixin(procedure_table, orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    '''Handling of common parts of procedures'''

    from qcexport import _general_copy

    # Fix keywords in the qc_spec column
    keyword_id = orm_obj.qc_spec['keywords']
    if keyword_id is not None:
        # Is it a hash? That is incorrect
        if not keyword_id.isdecimal():
            print(indent + f'!!! Keyword {keyword_id} is not an integer!')
        else:
            new_kw = _general_copy('keywords',
                                   session_dest,
                                   session_src,
                                   new_pk_map,
                                   options,
                                   filter_by={'id': src_info['qc_spec']['keywords']},
                                   single=True,
                                   indent=indent + '  ')

            orm_obj.qc_spec['keywords'] = new_kw['id']


def _add_optimization_procedure(orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    from qcexport import _general_copy

    print(indent + f'$ Adding extra children for optimization procedure {src_info["id"]}')

    _general_copy('opt_result_association',
                  session_dest,
                  session_src,
                  new_pk_map,
                  options,
                  filter_by={'opt_id': src_info['id']},
                  indent=indent + '  ')

    _add_procedure_mixin('optimization_procedure', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent)


def _add_gridoptimization_procedure(orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    from qcexport import _general_copy

    print(indent + f'$ Adding extra children for grid optimization procedure {src_info["id"]}')

    _general_copy('grid_optimization_association',
                  session_dest,
                  session_src,
                  new_pk_map,
                  options,
                  filter_by={'grid_opt_id': src_info['id']},
                  indent=indent + '  ')

    _add_procedure_mixin('grid_optimization_procedure', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent)

def _add_torsiondrive_procedure(orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    from qcexport import _general_copy


    print(indent + f'$ Adding extra children for torsiondrive procedure {src_info["id"]}')

    _general_copy('torsion_init_mol_association',
                  session_dest,
                  session_src,
                  new_pk_map,
                  options,
                  filter_by={'torsion_id': src_info['id']},
                  indent=indent + '  ')

    _add_procedure_mixin('torsiondrive_procedure', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent)


def _add_queuemanager(orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    '''Adds extra info for queue managers (ie, logs)'''

    from qcexport import _general_copy

    print(indent + f'$ Adding extra children for queue manager {src_info["id"]}:{src_info["name"]}')

    max_limit = options.get('queue_manager_log_max', None)

    # Add the logs for the queue manager
    _general_copy(table_name='queue_manager_logs',
                  session_dest=session_dest,
                  session_src=session_src,
                  new_pk_map=new_pk_map,
                  options=options,
                  filter_by={'manager_id': src_info['id']},
                  order_by={'id': 'desc'},
                  limit=max_limit,
                  indent=indent + '  ')


extra_children_map = {CollectionORM: _add_collection,
                      QueueManagerORM: _add_queuemanager,
                      OptimizationProcedureORM: _add_optimization_procedure,
                      GridOptimizationProcedureORM: _add_gridoptimization_procedure,
                      TorsionDriveProcedureORM: _add_torsiondrive_procedure,
                     }
