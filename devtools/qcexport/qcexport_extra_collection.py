'''
Handles special relationships for collections
'''

##########################################
# TODO - fix import mess
# Cannot import _general_copy here since it
# will result in a circular import
##########################################

def _add_dataset(entry_table, orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    '''Adds data for a dataset not specified by foreign keys

    Datasets are loosely coupled to results

    The src_info represents the data as given in the source database (before changing ids, etc)
    '''

    from qcexport import _general_copy

    # First add the entries (molecules) corresponding to this dataset (dataset_entry table)
    # Look up the maximum number of entries (molecules)
    max_entries = options.get('dataset_max_entries', None)
    entry_info = _general_copy(entry_table,
                               session_dest,
                               session_src,
                               new_pk_map,
                               options,
                               filter_by={'dataset_id': src_info['id']},
                               limit=max_entries,
                               indent=indent + '  ')

    # Loop over alias_keywords and add them to the db, updating to the corrext keyword id
    for program, aliases in orm_obj.alias_keywords.items():
        for alias_name, keyword_id in aliases.items():
            # Add the keywords (if they exist)
            if keyword_id is not None:
                new_kw = _general_copy('keywords',
                                       session_dest,
                                       session_src,
                                       new_pk_map,
                                       options,
                                       filter_by={'id': keyword_id},
                                       single=True,
                                       indent=indent + '  ')
                
                aliases[alias_name] = new_kw['id']

    # Loop through molecules and history to find all the results and copy those
    # But only those corresponding to the entries (molecules) we copied above (searched by name)
    added_molecules = [x['name'] for x in entry_info]
    molecules = [x['molecule_id'] for x in src_info['records'] if x['name'] in added_molecules]

    for history in src_info['history']:
        history = {k: v for k, v in zip(src_info['history_keys'], history)}
        program = history['program']
        keywords = history['keywords']

        ###################################################
        # Look up the id of the keywords in alias_keywords
        # And replace in the history dict
        # This isn't stored back to the ORM object, but is used for looking up results

        # Sometimes, something is not in this alias_keywords dict. Something got lost somewhere
        # (sometimes happens with dftd3)
        try:
            keywords_id = src_info['alias_keywords'][program][keywords]
        except KeyError:
            print(indent + f'!!! Alias keywords not found! ["{program}"]["{keywords}"]')
            continue

        history['keywords'] = keywords_id

        for molecule in molecules:
            filter_by = history.copy()
            filter_by['molecule'] = molecule

            # Find and add the corresponding result
            # We don't store the id anywhere, though
            _general_copy(table_name='result',
                          session_dest=session_dest,
                          session_src=session_src,
                          new_pk_map=new_pk_map,
                          options=options,
                          filter_by=filter_by,
                          indent=indent)


def _add_proceduredataset(procedure_table, orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    from qcexport import _general_copy

    # Data is stored in the 'object_map' member of 'records' dict
    # Create a new one with updated procedure IDs

    records = orm_obj.extra.pop('records')
    orm_obj.extra['records'] = {}

    max_entries = options.get('dataset_max_entries', None)

    # Fix the keywords in qc_spec
    for spec_name,spec in orm_obj.extra['specs'].items():
        qc_keyword_id = spec['qc_spec']['keywords']
        if qc_keyword_id is not None:
            new_qc_kw = _general_copy('keywords',
                                      session_dest,
                                      session_src,
                                      new_pk_map,
                                      options,
                                      filter_by={'id': qc_keyword_id},
                                      single=True,
                                      indent=indent + '  ')

            spec['qc_spec']['keywords'] = new_qc_kw['id']

    # Now go through all the records
    for count,(key,record) in enumerate(records.items()):
        # Remember that count goes starts at zero, therefore >=
        if max_entries is not None and count >= max_entries:
            break

        # Add the initial molecule
        # Sometimes stored as initial_molecule, initial_molecules, starting_molecule
        for initial_key in ['initial_molecule', 'initial_molecules', 'starting_molecule']:
            mol_id = record.get(initial_key, None)

            # Did not exist
            if mol_id is None:
                continue

            if isinstance(mol_id, list):
                initial_molecule = _general_copy(table_name='molecule',
                                                 session_dest=session_dest,
                                                 session_src=session_src,
                                                 new_pk_map=new_pk_map,
                                                 options=options,
                                                 filter_in={'id': mol_id},
                                                 single=False,
                                                 indent=indent + '  ')

                record[initial_key] = [x["id"] for x in initial_molecule]
            else:
                initial_molecule = _general_copy(table_name='molecule',
                                                 session_dest=session_dest,
                                                 session_src=session_src,
                                                 new_pk_map=new_pk_map,
                                                 options=options,
                                                 filter_by={'id': mol_id},
                                                 single=True,
                                                 indent=indent + '  ')

                record[initial_key] = initial_molecule['id']

        new_object_map = {}
        for spec_name, procedure_id in record['object_map'].items():
            print(indent + f'* Adding spec {spec_name} procedure id {procedure_id}')

            filter_by = {'id': procedure_id}
            opt_procedure = _general_copy(table_name=procedure_table,
                                          session_dest=session_dest,
                                          session_src=session_src,
                                          new_pk_map=new_pk_map,
                                          options=options,
                                          filter_by=filter_by,
                                          single=True,
                                          indent=indent + '  ')
            new_object_map[spec_name] = opt_procedure['id']

        record['object_map'] = new_object_map
        orm_obj.extra['records'][key] = record
            

def _add_collection(orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent):
    '''Handles adding loosely-coupled children of collections'''
    from qcexport import _general_copy

    collection_type = src_info['collection']

    print(indent + f'$ Adding extra children for collection {src_info["id"]} of type {collection_type}')
    if collection_type == 'dataset':
        _add_dataset('dataset_entry', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent + '  ')
    elif collection_type == 'reactiondataset':
        _add_dataset('reaction_dataset_entry', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent + '  ')
    elif collection_type == 'optimizationdataset':
        _add_proceduredataset('optimization_procedure', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent + '  ')
    elif collection_type == 'torsiondrivedataset':
        _add_proceduredataset('torsiondrive_procedure', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent + '  ')
    elif collection_type == 'gridoptimizationdataset':
        _add_proceduredataset('grid_optimization_procedure', orm_obj, src_info, session_dest, session_src, new_pk_map, options, indent + '  ')
    else:
        raise RuntimeError(f"Unknown collection type {collection_type}")
