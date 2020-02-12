"""
A module for searching for TS guesses using RMG-family-specific heuristics

Todo:
    - eventually this module needs to be applied for all H's connected to the same heavy atom,
      summing up the rates appropriately
    - test H2O2 as the RH abstractor, see that both TS chiralities are attained
"""

import itertools

from zmats import compare_zmats, get_parameter_from_atom_indices, is_angle_linear, up_param

from rmgpy.data.kinetics.database import KineticsDatabase
from rmgpy.data.kinetics.family import KineticsFamily
from rmgpy.data.rmg import RMGDatabase
from rmgpy.exceptions import ActionError
from rmgpy.reaction import Reaction

from arc.common import colliding_atoms, key_by_val
from arc.species.converter import zmat_from_xyz, zmat_to_xyz

from .factory import register_ts_adapter
from .ts_adapter import TSAdapter


IMPLEMENTED_FAMILIES = ['H_Abstraction']


class HeuristicsAdapter(TSAdapter):
    """
    A class for representing brute-force heuristics for transition state guesses.
    """

    def __init__(self, user_guesses: list = None,
                 rmg_reaction: Reaction = None,
                 dihedral_increment: float = 20,
                 ) -> None:
        """
        Initializes a HeuristicsAdapter instance.

        Parameters
        ----------
        user_guesses : list
            TS user guesses, not used in the HeuristicsAdapter class.
        rmg_reaction: Reaction, optional
            The RMG Reaction object, not used in the UserAdapter class.
        dihedral_increment: float, optional
            The scan dihedral increment to use when generating guesses.
        """
        if rmg_reaction is not None and not isinstance(rmg_reaction, Reaction):
            raise TypeError(f'rmg_reaction must be an RMG Reaction instance, got\n'
                            f'{rmg_reaction}\n'
                            f'which is a {type(rmg_reaction)}.')
        self.rmg_reaction = rmg_reaction
        self.dihedral_increment = dihedral_increment

    def __repr__(self) -> str:
        """A short representation of the current HeuristicsAdapter.

        Returns
        -------
        str
            The desired representation.
        """
        return f"HeuristicsAdapter(rmg_reaction={self.rmg_reaction}, dihedral_increment={self.dihedral_increment})"

    def generate_guesses(self) -> list:
        """
        Generate TS guesses using heuristics according to the respective RMG reaction family.

        Returns
        -------
        list
            Entries are TS guess dictionaries.

        # Todo: adapt `arc_reaction` to qcf's syntax
        """
        if self.rmg_reaction is None:
            return list()

        results = list()

        if self.rmg_reaction.family is None:
            print('Error: Cannot generate a TS guess for a reaction using heuristics without an RMG family attribute.')
            return list()

        if self.rmg_reaction.family.label not in IMPLEMENTED_FAMILIES:
            print(f'Error: Heuristics-based TS guesses generation for reaction family '
                  f'{self.rmg_reaction.family} is not implemented yet.')

        arc_reaction.arc_species_from_rmg_reaction()
        reactant_mol_combinations = list(
            itertools.product(*list(reactant.mol_list for reactant in arc_reaction.r_species)))
        product_mol_combinations = list(
            itertools.product(*list(product.mol_list for product in arc_reaction.p_species)))
        reaction_list = list()
        for reactants in list(reactant_mol_combinations):
            for products in list(product_mol_combinations):
                reaction = label_molecules(list(reactants), list(products), arc_reaction.family)
                if reaction is not None:
                    reaction_list.append(reaction)
        results = list()

        if self.rmg_reaction.family.label == 'H_Abstraction':
            results = h_abstraction(arc_reaction, reaction_list, dihedral_increment=dihedral_increment)
        return results







def generate_guesses(arc_reaction, dihedral_increment=20):
    """
    Generate several TS guesses according to the RMG reaction family.

    Args:
        arc_reaction (ARCReaction): The reaction to generate TS guesses for. The .family attribute must be populated.
        dihedral_increment (float, optional): The dihedral increment to use for generating the guesses.

    Returns:
        list: Entries are xyz guesses for the TS.
    """
    if arc_reaction.family is None:
        print('Error: Cannot generate a TS guess for a reaction using heuristics without an RMG family attribute')
        return list()
    if arc_reaction.family.label not in IMPLEMENTED_FAMILIES:
        raise NotImplementedError(f'Heuristics-based TS guess for reaction family {arc_reaction.family} '
                                  f'is not implemented yet.')

    arc_reaction.arc_species_from_rmg_reaction()
    reactant_mol_combinations = list(itertools.product(*list(reactant.mol_list for reactant in arc_reaction.r_species)))
    product_mol_combinations = list(itertools.product(*list(product.mol_list for product in arc_reaction.p_species)))
    reaction_list = list()
    for reactants in list(reactant_mol_combinations):
        for products in list(product_mol_combinations):
            reaction = label_molecules(list(reactants), list(products), arc_reaction.family)
            if reaction is not None:
                reaction_list.append(reaction)
    xyz_guesses = list()

    if arc_reaction.family.label == 'H_Abstraction':
        xyz_guesses = h_abstraction(arc_reaction, reaction_list, dihedral_increment=dihedral_increment)

    return xyz_guesses


def combine_coordinates_with_redundant_atoms(xyz1, xyz2, mol1, mol2, h1, h2, c=None, d=None,
                                             r1_stretch=1.2, r2_stretch=1.2, a2=180, d2=None, d3=None,
                                             keep_dummy=False):
    """
    Combine two coordinates that share an atom.
    For this redundant atom case, only three additional degrees of freedom (here ``a2``, ``d2``, and ``d3``)
    are required. The number of atoms in ``mol2`` should be lesser than or equal to the number of atoms in ''mol1''.

    Atom scheme (dummy atom X will be added if the B-H-A angle is close to 180 degrees)::

                    X           D
                    |         /
            A -- H1 - H2 -- B
          /
        C

        |--- mol1 --|-- mol2 ---|

    zmats will be created in the following way::

        zmat1 = {'symbols': ('C', 'H', 'H', 'H', 'H'),
                 'coords': ((None, None, None),  # 0, atom A
                            ('R_1_0', None, None),  # 1, atom C
                            ('R_2_1', 'A_2_1_0', None),  # 2
                            ('R_3_2', 'A_3_2_0', 'D_3_2_0_1'),  # 3
                            ('R_4_3' * r1_stretch, 'A_4_3_2', 'D_4_3_2_1')),  # 4, H1
                 'vars': {...},
                 'map': {...}}

        zmat2 = {'symbols': ('H', 'H', 'H', 'H', 'C'),
                 'coords': ((None, None, None),  # H2, redundant H atom, will be united with H1
                            ('R_5_4' * r2_stretch, a2 (B-H-A) = 'A_5_4_0', d2 (B-H-A-C) = 'D_5_4_0_1'),  # 5, atom B
                            ('R_6_5', 'A_6_5_4', d3 (D-B-H-C) = 'D_6_5_4_1'),  # 6, atom D
                            ('R_7_6', 'A_7_6_4', 'D_7_6_4_5'),  # 7
                            ('R_8_7', 'A_8_7_6', 'D_8_7_6_5')),  # 8
                 'vars': {...},
                 'map': {...}}


    Args:
        xyz1 (dict, str): The Cartesian coordinates of molecule 1 (including the redundant atom).
        xyz2 (dict, str): The Cartesian coordinates of molecule 2 (including the redundant atom).
        mol1 (Molecule): The Molecule instance corresponding to ``xyz1``.
        mol2 (Molecule): The Molecule instance corresponding to ``xyz2``.
        h1 (int): The 0-index of a terminal redundant atom in ``xyz1`` (atom H1).
        h2 (int): The 0-index of a terminal redundant atom in ``xyz2`` (atom H2).
        c (int, optional): The 0-index of an atom in ``xyz1`` connected to either A or H1 which is neither A nor H1
                           (atom C).
        d (int, optional): The 0-index of an atom in ``xyz2`` connected to either B or H2 which is neither B nor H2
                           (atom D).
        r1_stretch (float, optional): The factor by which to multiply (stretch/shrink) the bond length to the terminal
                                      atom ``h1`` in ``xyz1`` (bond A-H1).
        r2_stretch (float, optional): The factor by which to multiply (stretch/shrink) the bond length to the terminal
                                      atom ``h2`` in ``xyz2`` (bond B-H2).
        a2 (float, optional): The angle (in degrees) in the combined structure between atoms B-H-A (angle B-H-A).
        d2 (float, optional): The dihedral angle (in degrees) between atoms B-H-A-C (dihedral B-H-A-C).
                              This argument must be given only if the a2 angle is not linear,
                              and mol2 has 3 or more atoms, otherwise it is meaningless.
        d3 (float, optional): The dihedral angel (in degrees) between atoms D-B-H-C (dihedral D-B-H-C).
                              This parameter is mandatory only if atom D exists (i.e., if ``mol2`` has 3 or more atoms).
        keep_dummy (bool, optional): Whether to keep a dummy atom if added, ``True`` to keep, ``False`` by default.

    Returns:
        dict: The combined cartesian coordinates.

    Todo:
        Accept xyzs of the radicals as well as E0's of all species, average xyz of atoms by energy similarity
        before returning the final cartesian coordinates
    """
    is_a2_linear = is_angle_linear(a2)

    if len(mol1.atoms) == 1 or len(mol2.atoms) == 1:
        raise ValueError(f'The molecule arguments to combine_coordinates_with_redundant_atoms must each have more than 1 '
                      f'atom (including the abstracted hydrogen atom in each), got {len(mol1.atoms)} atoms in mol1 '
                      f'and {len(mol2.atoms)} atoms in mol2.')
    if not is_a2_linear and len(mol1.atoms) > 2 and d2 is None:
        raise ValueError('The d2 parameter (the B-H-A-C dihedral) must be given if the a2 angle (B-H-A) is not close '
                      'to 180 degrees, got None.')
    if is_angle_linear(a2) and d2 is not None:
        print(f'Warning: The combination a2={a2} and d2={d2} is meaningless (cannot rotate a dihedral about a linear '
              f'angle). Not considering d2.')
        d2 = None
    if len(mol1.atoms) > 2 and c is None:
        raise ValueError('The c parameter (the index of atom C in xyz1) must be given if mol1 has 3 or more atoms, '
                      'got None.')
    if len(mol2.atoms) > 2 and d is None:
        raise ValueError('The d parameter (the index of atom D in xyz2) must be given if mol2 has 3 or more atoms, '
                      'got None.')
    if len(mol2.atoms) > 2 and d3 is None:
        raise ValueError('The d3 parameter (dihedral D-B-H-C) must be given if mol2 has 3 or more atoms, got None.')

    a = mol1.atoms.index(list(mol1.atoms[h1].edges.keys())[0])
    b = mol2.atoms.index(list(mol2.atoms[h2].edges.keys())[0])
    if c is not None and c == a:
        raise ValueError(f'The value for c ({c}) is invalid (it represents atom A, not atom C)')
    if c is not None and d == b:
        raise ValueError(f'The value for d ({d}) is invalid (it represents atom B, not atom D)')

    # generate the two constrained zmats
    constraints1 = {'R_atom': [(h1, a)]}
    zmat1 = zmat_from_xyz(xyz=xyz1, mol=mol1, constraints=constraints1, consolidate=False)

    constraints2 = {'A_group': [(d, b, h2)]} if d is not None else {'R_group': [(b, h2)]}
    zmat2 = zmat_from_xyz(xyz=xyz2, mol=mol2, constraints=constraints2, consolidate=False)

    # stretch the A--H1 and B--H2 bonds
    r_a_h1_param = get_parameter_from_atom_indices(zmat=zmat1, indices=(h1, a), xyz_indexed=True)
    r_b_h2_param = get_parameter_from_atom_indices(zmat=zmat2, indices=(b, h2), xyz_indexed=True)
    zmat1['vars'][r_a_h1_param] *= r1_stretch
    zmat2['vars'][r_b_h2_param] *= r2_stretch

    # determine the "glue" parameters
    num_atoms_1 = len(zmat1['symbols'])  # the number of atoms in zmat1, used to increment the atom indices in zmat2
    zh = num_atoms_1 - 1  # the atom index of H in the combined zmat
    za = key_by_val(zmat1['map'], a)  # the atom index of A in the combined zmat
    zb = num_atoms_1 + int(is_a2_linear)  # the atom index of B in the combined zmat, if a2=180, consider the dummy atom
    zc = key_by_val(zmat1['map'], c) if c is not None else None
    zd = num_atoms_1 + 1 + int(is_a2_linear) if d is not None else None  # the atom index of B in the combined zmat
    param_a2 = f'A_{zb}_{zh}_{za}'  # B-H-A
    param_d2 = f'D_{zb}_{zh}_{za}_{zc}' if zc is not None else None  # B-H-A-C
    if is_a2_linear:
        # add a dummy atom
        zx = num_atoms_1
        num_atoms_1 += 1
        zmat1['symbols'] = tuple(list(zmat1['symbols']) + ['X'])
        r_str = f'RX_{zx}_{zh}'
        a_str = f'AX_{zx}_{zh}_{za}'
        d_str = f'DX_{zx}_{zh}_{za}_{zc}' if zc is not None else None  # X-H-A-C
        zmat1['coords'] = tuple(list(zmat1['coords']) + [(r_str, a_str, d_str)])  # the coords of the dummy atom
        zmat1['vars'][r_str] = 1.0
        zmat1['vars'][a_str] = 90.0
        if d_str is not None:
            zmat1['vars'][d_str] = 0
        param_a2 = f'A_{zb}_{zh}_{zx}'  # B-H-X
        param_d2 = f'D_{zb}_{zh}_{zx}_{za}' if zc is not None else None  # B-H-X-A
    if d3 is not None and zd is not None:
        param_d3 = f'D_{zd}_{zb}_{zh}_{zc}'  # D-B-H-C
    else:
        param_d3 = None

    # generate a modified zmat2: remove the first atom, change all existing parameter indices, add "glue" parameters
    new_coords, new_vars = list(), dict()
    for i, coords in enumerate(zmat2['coords'][1:]):
        new_coord = list()
        for j, param in enumerate(coords):
            if param is not None:
                if i == 0 and is_a2_linear:
                    # atom B should refer to H, not X
                    new_param = up_param(param=param, increment_list=[num_atoms_1 - 1, num_atoms_1 - 2])
                else:
                    new_param = up_param(param=param, increment=num_atoms_1 - 1)
                new_coord.append(new_param)
                new_vars[new_param] = zmat2['vars'][param]  # keep the original parameter R/A/D values
            else:
                if i == 0 and j == 1:
                    # this is a2
                    new_coord.append(param_a2)
                    new_vars[param_a2] = a2 + 90 if is_a2_linear else a2
                elif i == 0 and j == 2 and c is not None:
                    # this is d2
                    new_coord.append(param_d2)
                    new_vars[param_d2] = 0 if is_a2_linear else d2
                elif i == 1 and j == 2 and param_d3 is not None:
                    # this is d3
                    new_coord.append(param_d3)
                    new_vars[param_d3] = d3
                else:
                    new_coord.append(None)
        new_coords.append(tuple(new_coord))

    combined_zmat = dict()
    combined_zmat['symbols'] = tuple(zmat1['symbols'] + zmat2['symbols'][1:])
    combined_zmat['coords'] = tuple(list(zmat1['coords']) + new_coords)
    combined_zmat['vars'] = {**zmat1['vars'], **new_vars}  # combine the two dicts
    combined_zmat['map'] = dict()
    x_occurrences = 0
    for i, symbol in enumerate(combined_zmat['symbols']):
        if symbol == 'X':
            combined_zmat['map'][i] = 'X'
            x_occurrences += 1
        else:
            combined_zmat['map'][i] = i - x_occurrences

    for i, coords in enumerate(combined_zmat['coords']):
        if i > 2 and None in coords:
            raise ValueError(f'Could not combine zmats, got a None parameter above the 3rd row:\n{combined_zmat}')
    return zmat_to_xyz(zmat=combined_zmat, keep_dummy=keep_dummy)




def label_molecules(reactants, products, family, output_with_resonance=False):
    """
    React molecules to give the requested products via an RMG family.
    Results in a reaction with RMG's atom labels for the reactants and products.

    Args:
        reactants (list): Entries are Molecule instances of the reaction reactants.
        products (list): Entries are Molecule instances of the reaction products.
        family (KineticsFamily): The RMG reaction family instance.
        output_with_resonance (bool, optional): Whether to generate all resonance structures with labels.
                                                ``True`` to generate``, ``False`` by default.

    Returns:
        Reaction: An RMG Reaction instance with atom-labeled reactants and products.
    """
    reaction = Reaction(reactants=reactants, products=products)
    try:
        family.add_atom_labels_for_reaction(reaction=reaction, output_with_resonance=output_with_resonance)
    except ActionError:
        return None
    return reaction


# family-specific heuristics


def h_abstraction(arc_reaction, rmg_reactions, r1_stretch=1.2, r2_stretch=1.2, a2=180, dihedral_increment=20):
    """
    Generate TS guesses for reactions of the RMG H_Abstraction family.

    Args:
        arc_reaction: An ARCReaction instance.
        rmg_reactions: Entries are RMG Reaction instances. The reactants and products attributes should not contain
                       resonance structures as only the first molecule is consider - pass several Reaction entries
                       instead. Atoms must be labeled according to the RMG reaction family.
        r1_stretch (float, optional): The factor by which to multiply (stretch/shrink) the bond length to the terminal
                                      atom ``h1`` in ``xyz1`` (bond A-H1).
        r2_stretch (float, optional): The factor by which to multiply (stretch/shrink) the bond length to the terminal
                                      atom ``h2`` in ``xyz2`` (bond B-H2).
        a2 (float, optional): The angle (in degrees) in the combined structure between atoms B-H-A (angle B-H-A).
        dihedral_increment (float, optional): The dihedral increment to use for B-H-A-C and D-B-H-C dihedral scans.

    Returns:
        list: Entries are Cartesian coordinates of TS guesses for all reactions.

    # Todo: make sure this function returns the desired format in the new implementation
    """
    xyz_guesses = list()

    # identify R1H and R2H in "R1H + R2 <=> R1 + R2H" for the ARC reaction:
    arc_reactant = sorted(arc_reaction.r_species, key=lambda x: x.multiplicity, reverse=False)[0]
    arc_product = sorted(arc_reaction.p_species, key=lambda x: x.multiplicity, reverse=False)[0]

    for rmg_reaction in rmg_reactions:
        # identify R1H and R2H in "R1H + R2 <=> R1 + R2H" for the RMG reaction:
        rmg_reactant_mol = sorted(rmg_reaction.reactants, key=lambda x: x.multiplicity, reverse=False)[0].molecule[0]
        rmg_product_mol = sorted(rmg_reaction.products, key=lambda x: x.multiplicity, reverse=False)[0].molecule[0]

        h1 = rmg_reactant_mol.atoms.index([atom for atom in rmg_reactant_mol.atoms
                                           if atom.label == '*2'][0])
        h2 = rmg_product_mol.atoms.index([atom for atom in rmg_product_mol.atoms
                                          if atom.label == '*2'][0])

        c, d = None, None

        # atom C is the 0-index of an atom in ``xyz1`` connected to either A or H1 which is neither A nor H1
        if len(rmg_reactant_mol.atoms) > 2:
            found_c = False
            a = None
            # search for atom C connected to atom A:
            for atom_a in rmg_reactant_mol.atoms[h1].edges.keys():
                for atom_c in atom_a.edges.keys():
                    if rmg_reactant_mol.atoms.index(atom_c) != h1:
                        a = rmg_reactant_mol.atoms.index(atom_a)
                        c = rmg_reactant_mol.atoms.index(atom_c)
                        found_c = True
                        break
                if not found_c:
                    for atom in rmg_reactant_mol.atoms:
                        if rmg_reactant_mol.atoms.index(atom) not in [h1, a]:
                            c = rmg_reactant_mol.atoms.index(atom)
                            break
                break

        # atom D is the 0-index of an atom in ``xyz2`` connected to either B or H2 which is neither B nor H2
        if len(rmg_product_mol.atoms) > 2:
            found_d = False
            b = None
            # search for atom D connected to atom B:
            for atom_b in rmg_product_mol.atoms[h2].edges.keys():
                for atom_d in atom_b.edges.keys():
                    if rmg_product_mol.atoms.index(atom_d) != h2:
                        b = rmg_product_mol.atoms.index(atom_b)
                        d = rmg_product_mol.atoms.index(atom_d)
                        found_d = True
                        break
                if not found_d:
                    for atom in rmg_product_mol.atoms:
                        if rmg_product_mol.atoms.index(atom) not in [h2, b]:
                            d = rmg_product_mol.atoms.index(atom)
                            break
                break

        # d2 describes the B-H-A-C dihedral, populate d2_values if C exists and the B-H-A angle is not linear
        d2_values = list(range(0, 360, dihedral_increment)) if len(rmg_reactant_mol.atoms) > 2 \
            and not is_angle_linear(a2) else list()

        # d3 describes the D-B-H-C dihedral, populate d3_values if D and C exist
        d3_values = list(range(0, 360, dihedral_increment)) if len(rmg_product_mol.atoms) > 2 \
            and len(rmg_product_mol.atoms) > 2 else list()

        if d2_values and d3_values:
            d2_d3_product = list(itertools.product(d2_values, d3_values))
        elif d2_values:
            d2_d3_product = [(d2, None) for d2 in d2_values]
        elif d3_values:
            d2_d3_product = [(None, d3) for d3 in d3_values]
        else:
            d2_d3_product = [(None, None)]

        # Todo:
        # r1_stretch_, r2_stretch_, a2_ = get_training_params(
        #     family='H_Abstraction',
        #     atom_type_key=tuple(sorted([atom_a.atomtype.label, atom_b.atomtype.label])),
        #     atom_symbol_key=tuple(sorted([atom_a.element.symbol, atom_b.element.symbol])),
        # )
        r1_stretch_, r2_stretch_, a2_ = 1.2, 1.2, 170  # general guesses

        zmats = list()
        for d2, d3 in d2_d3_product:
            xyz_guess = combine_coordinates_with_redundant_atoms(xyz1=arc_reactant.get_xyz(),
                                                                 xyz2=arc_product.get_xyz(),
                                                                 mol1=arc_reactant.mol,
                                                                 mol2=arc_product.mol,
                                                                 h1=h1,
                                                                 h2=h2,
                                                                 c=c,
                                                                 d=d,
                                                                 r1_stretch=r1_stretch,
                                                                 r2_stretch=r2_stretch,
                                                                 a2=a2,
                                                                 d2=d2,
                                                                 d3=d3)

            if not colliding_atoms(xyz_guess):  # len(qcel.molutil.guess_connectivity(symbols, geometry, threshold=0.9))
                zmat_guess = zmat_from_xyz(xyz_guess)
                for existing_zmat_guess in zmats:
                    if compare_zmats(existing_zmat_guess, zmat_guess):
                        break
                else:
                    # this TS is unique, and has no atom collisions
                    zmats.append(zmat_guess)
                    xyz_guesses.append(xyz_guess)

    # learn bond stretches and the A-H-B angle for different atom types
    return xyz_guesses


register_ts_adapter('heuristics', HeuristicsAdapter)

