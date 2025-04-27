from qcportal.molecules import Molecule, MoleculeUploadOptions

from typing import Iterable, List, Tuple


def molecules_from_files(
    file_list: Iterable[Tuple[str, str]], options: MoleculeUploadOptions
) -> List[Tuple[str, Molecule]]:
    return [(fname, Molecule.from_file(fpath)) for fname, fpath in file_list]
