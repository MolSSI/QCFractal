"""
A module for interacting with the Reaction Mechanism Generator database.
"""

import os

from rmgpy import settings
from rmgpy.data.kinetics.common import find_degenerate_reactions
from rmgpy.data.kinetics.family import KineticsFamily
from rmgpy.data.rmg import RMGDatabase
from rmgpy.exceptions import KineticsError
from rmgpy.reaction import same_species_lists, Reaction

RMG_DB_PATH = settings['database.directory']


def make_rmg_database_object():
    """
    Make a clean RMGDatabase object.

    Returns:
        RMGDatabase: A clean RMG database object.
    """
    rmg_db = RMGDatabase()
    return rmg_db


def load_families_only(rmg_db, kinetics_families='default'):
    """
    A helper function for loading kinetic families from RMG's database.

    Args:
        rmg_db (RMGDatabase): The RMG database instance.
        kinetics_families (str, optional): Specific kinetics families to load.
    """
    if kinetics_families not in ('default', 'all', 'none') and not isinstance(kinetics_families, list):
            raise ValueError(f"kinetics families should be either 'default', 'all', 'none', or a list of names, e.g., "
                             f"['H_Abstraction', 'R_Recombination'] or ['!Intra_Disproportionation']. "
                             f"Got:\n{kinetics_families}")
    rmg_db.load(
        path=RMG_DB_PATH,
        thermo_libraries=list(),
        transport_libraries='none',
        reaction_libraries=list(),
        seed_mechanisms=list(),
        kinetics_families=kinetics_families,
        kinetics_depositories=['training'],
        depository=False,
    )


def load_rmg_database(rmg_db, thermo_libraries=None, reaction_libraries=None, kinetics_families='default',
                      load_thermo_libs=True, load_kinetic_libs=True, include_nist=False):
    """
    A helper function for loading the RMG database.

    Args:
        rmg_db (RMGDatabase): The RMG database instance.
        thermo_libraries (list, optional): Specific thermodynamic libraries to load (``None`` will load all).
        reaction_libraries (list, optional): Specific kinetics libraries to load (``None`` will load all).
        kinetics_families (list, str, optional): Specific kinetics families to load
                                                 (either a list or 'default', 'all', 'none')
        load_thermo_libs (bool, optional): Whether to load thermodynamic libraries, ``True`` to load.
        load_kinetic_libs (bool, optional): Whether to load kinetics libraries, ``True`` to load.
        include_nist (bool, optional): Whether to include the NIST kinetics libraries,
                                      ``True`` to include, default is ``False``
    """
    thermo_libraries = thermo_libraries if thermo_libraries is not None else list()
    reaction_libraries = reaction_libraries if reaction_libraries is not None else list()
    if isinstance(thermo_libraries, str):
        thermo_libraries.replace(' ', '')
        thermo_libraries = [lib for lib in thermo_libraries.split(',')]
    if isinstance(reaction_libraries, str):
        reaction_libraries.replace(' ', '')
        reaction_libraries = [lib for lib in reaction_libraries.split(',')]
        reaction_libraries = [reaction_libraries]
    if kinetics_families not in ('default', 'all', 'none'):
        if not isinstance(kinetics_families, list):
            raise ValueError("kinetics_families should be either 'default', 'all', 'none', or a list of names, e.g., "
                             "['H_Abstraction','R_Recombination'] or ['!Intra_Disproportionation'].")
    if not thermo_libraries:
        thermo_path = os.path.join(RMG_DB_PATH, 'thermo', 'libraries')
        for thermo_library_path in os.listdir(thermo_path):
            # Avoid reading .DS_store files for compatibility with Mac OS
            if not thermo_library_path.startswith('.'):
                thermo_library, _ = os.path.splitext(os.path.basename(thermo_library_path))
                thermo_libraries.append(thermo_library)
        # prioritize libraries
        thermo_priority = ['BurkeH2O2', 'thermo_DFT_CCSDTF12_BAC', 'DFT_QCI_thermo', 'Klippenstein_Glarborg2016',
                           'primaryThermoLibrary', 'primaryNS', 'NitrogenCurran', 'NOx2018', 'FFCM1(-)',
                           'SulfurLibrary', 'SulfurGlarborgH2S']
        indices_to_pop = []
        for i, lib in enumerate(thermo_libraries):
            if lib in thermo_priority:
                indices_to_pop.append(i)
        for i in reversed(range(len(thermo_libraries))):  # pop starting from the end, so other indices won't change
            if i in indices_to_pop:
                thermo_libraries.pop(i)
        thermo_libraries = thermo_priority + thermo_libraries

    if not reaction_libraries:
        kinetics_path = os.path.join(RMG_DB_PATH, 'kinetics', 'libraries')
        # Avoid reading .DS_store files for compatibility with Mac OS
        reaction_libraries = [library for library in os.listdir(kinetics_path) if not library.startswith('.')]
        indices_to_pop = list()
        second_level_libraries = list()
        for i, library in enumerate(reaction_libraries):
            if not os.path.isfile(os.path.join(kinetics_path, library, 'reactions.py')) and library != 'Surface':
                indices_to_pop.append(i)
                # Avoid reading .DS_store files for compatibility with Mac OS (`if not second_level.startswith('.')`)
                second_level_libraries.extend([library + '/' + second_level
                                               for second_level in os.listdir(os.path.join(kinetics_path, library))
                                               if not second_level.startswith('.')])
        for i in reversed(range(len(reaction_libraries))):  # pop starting from the end, so other indices won't change
            if i in indices_to_pop or reaction_libraries[i] == 'Surface':
                reaction_libraries.pop(i)
        reaction_libraries.extend(second_level_libraries)

    if not load_kinetic_libs:
        reaction_libraries = list()
    if not load_thermo_libs:
        thermo_libraries = list()

    kinetics_depositories = ['training', 'NIST'] if include_nist else ['training']
    rmg_db.load(
        path=RMG_DB_PATH,
        thermo_libraries=thermo_libraries,
        transport_libraries=['PrimaryTransportLibrary', 'NOx2018', 'GRI-Mech'],
        reaction_libraries=reaction_libraries,
        seed_mechanisms=list(),
        kinetics_families=kinetics_families,
        kinetics_depositories=kinetics_depositories,
        depository=False,
    )
    for family in rmg_db.kinetics.families.values():
        try:
            family.add_rules_from_training(thermo_database=rmg_db.thermo)
        except KineticsError:
            pass
        else:
            family.fill_rules_by_averaging_up(verbose=False)


def determine_reaction_family(rmg_db, reaction):
    """
    Determine the RMG kinetic family for a given ARCReaction object.
    Returns None if no family found or more than one family found.

    Args:
        rmg_db (RMGDatabase): The RMG database instance.
        reaction (Reaction): The RMG Reaction object.

    Returns:
        KineticsFamily: The corresponding RMG reaction family. None if 0 or more than 1 family were found.
    Returns:
         bool: Whether the family is its own reverse.
    """
    fam_list = loop_families(rmg_db=rmg_db, reaction=reaction)
    families = [fam_l[0] for fam_l in fam_list]
    if len(set(families)) == 1:
        return families[0], families[0].own_reverse
    else:
        return None, False


def loop_families(rmg_db, reaction):
    """
    Loop through kinetic families and return a list of tuples of (family, degenerate_reactions)
    `reaction` is an RMG Reaction object.
    Returns a list of (family, degenerate_reactions) tuples.

    Args:
        rmg_db (RMGDatabase): The RMG database instance.
        reaction (Reaction): The RMG Reaction object.

    Returns:
        list: Entries are corresponding RMG KineticsFamily instances.
    """
    fam_list = list()
    for family in rmg_db.kinetics.families.values():
        degenerate_reactions = list()
        family_reactions_by_r = list()  # family reactions for the specified reactants
        family_reactions_by_rnp = list()  # family reactions for the specified reactants and products

        if len(reaction.reactants) == 1:
            for reactant0 in reaction.reactants[0].molecule:
                fam_rxn = family.generate_reactions(reactants=[reactant0],
                                                    products=reaction.products)
                if fam_rxn:
                    family_reactions_by_r.extend(fam_rxn)
        elif len(reaction.reactants) == 2:
            for reactant0 in reaction.reactants[0].molecule:
                for reactant1 in reaction.reactants[1].molecule:
                    fam_rxn = family.generate_reactions(reactants=[reactant0, reactant1],
                                                        products=reaction.products)
                    if fam_rxn:
                        family_reactions_by_r.extend(fam_rxn)
        elif len(reaction.reactants) == 3:
            for reactant0 in reaction.reactants[0].molecule:
                for reactant1 in reaction.reactants[1].molecule:
                    for reactant2 in reaction.reactants[2].molecule:
                        fam_rxn = family.generate_reactions(reactants=[reactant0, reactant1, reactant2],
                                                            products=reaction.products)
                        if fam_rxn:
                            family_reactions_by_r.extend(fam_rxn)

        if len(reaction.products) == 1:
            for fam_rxn in family_reactions_by_r:
                for product0 in reaction.products[0].molecule:
                    if same_species_lists([product0], fam_rxn.products):
                        family_reactions_by_rnp.append(fam_rxn)
            degenerate_reactions = find_degenerate_reactions(rxn_list=family_reactions_by_rnp,
                                                             same_reactants=False,
                                                             kinetics_database=rmg_db.kinetics)
        elif len(reaction.products) == 2:
            for fam_rxn in family_reactions_by_r:
                for product0 in reaction.products[0].molecule:
                    for product1 in reaction.products[1].molecule:
                        if same_species_lists([product0, product1], fam_rxn.products):
                            family_reactions_by_rnp.append(fam_rxn)
            degenerate_reactions = find_degenerate_reactions(rxn_list=family_reactions_by_rnp,
                                                             same_reactants=False,
                                                             kinetics_database=rmg_db.kinetics)
        elif len(reaction.products) == 3:
            for fam_rxn in family_reactions_by_r:
                for product0 in reaction.products[0].molecule:
                    for product1 in reaction.products[1].molecule:
                        for product2 in reaction.products[2].molecule:
                            if same_species_lists([product0, product1, product2], fam_rxn.products):
                                family_reactions_by_rnp.append(fam_rxn)
            degenerate_reactions = find_degenerate_reactions(rxn_list=family_reactions_by_rnp,
                                                             same_reactants=False,
                                                             kinetics_database=rmg_db.kinetics)
        if degenerate_reactions:
            fam_list.append((family, degenerate_reactions))
    return fam_list


def determine_rmg_kinetics(rmg_db, reaction, dh_rxn298):
    """
    Determine kinetics for `reaction` (an RMG Reaction object) from RMG's database, if possible.
    Assigns a list of all matching entries from both libraries and families.
    Returns a list of all matching RMG reactions (both libraries and families) with a populated .kinetics attribute
    `rmg_db` is the RMG database instance
    `reaction` is an RMG Reaction object
    `dh_rxn298` is the heat of reaction at 298 K in J / mol
    """
    rmg_reactions = list()
    # Libraries:
    for library in rmg_db.kinetics.libraries.values():
        library_reactions = library.get_library_reactions()
        for library_reaction in library_reactions:
            if reaction.is_isomorphic(library_reaction):
                library_reaction.comment = 'Library: {0}'.format(library.label)
                rmg_reactions.append(library_reaction)
                break
    # Families:
    fam_list = loop_families(rmg_db, reaction)
    for family, degenerate_reactions in fam_list:
        for deg_rxn in degenerate_reactions:
            template = family.retrieve_template(deg_rxn.template)
            kinetics = family.estimate_kinetics_using_rate_rules(template)[0]
            kinetics.change_rate(deg_rxn.degeneracy)
            kinetics = kinetics.to_arrhenius(dh_rxn298)  # Convert ArrheniusEP to Arrhenius using the dHrxn at 298K
            deg_rxn.kinetics = kinetics
            deg_rxn.comment = 'Family: {0}'.format(deg_rxn.family)
            deg_rxn.reactants = reaction.reactants
            deg_rxn.products = reaction.products
        rmg_reactions.extend(degenerate_reactions)
    worked_through_nist_fams = []
    # NIST:
    for family, degenerate_reactions in fam_list:
        if family not in worked_through_nist_fams:
            worked_through_nist_fams.append(family)
            for depo in family.depositories:
                if 'NIST' in depo.label:
                    for entry in depo.entries.values():
                        rxn = entry.item
                        rxn.kinetics = entry.data
                        rxn.comment = 'NIST: {0}'.format(entry.index)
                        if entry.reference is not None:
                            rxn.comment += '{0} {1}'.format(entry.reference.authors[0], entry.reference.year)
                        rmg_reactions.append(rxn)
    return rmg_reactions
