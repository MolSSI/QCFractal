import json
import os
import tarfile
import zipfile
from typing import Iterable, List, Tuple, Dict

from qcportal.molecules import Molecule, MoleculeUploadOptions


def _molecule_from_file(fname, data: str) -> Molecule:
    # Split into a separate function for future expansion
    if fname.lower().endswith(".json"):
        mol_data = json.loads(data)
        return Molecule(**mol_data)
    else:
        return Molecule.from_data(data)


def molecules_from_files(
    file_list: Iterable[Tuple[str, str]], options: MoleculeUploadOptions
) -> Tuple[Dict[str, List[Tuple[str, Molecule]]], List[str]]:
    """
    Load molecules from a list of files, supporting archives.

    Parameters
    ----------
    file_list
        Iterable of (filename, file_path). Filename includes extension to determine type.
    options
        Upload options (currently unused, reserved for future behavior tweaks).

    Returns
    -------
    :
        A tuple. The first part is a list of errors found when processing the files.
        The second part is a dictionary of filenames to list of molecule info. The molecule info is a tuple
        containing the inner filename and a Molecule object.

    Notes
    -----
    - Archives are processed in-memory; no extraction to filesystem occurs.
    - Directories within archives are ignored (no recursion).
    - Supported archives: .zip, .tar, .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.xz, .txz
    """

    errors: List[str] = []
    results: Dict[str, List[Tuple[str, Molecule]]] = {}

    for fname, fpath in file_list:
        results[fname] = []

        lower_fname = fname.lower()

        # ZIP archives
        if lower_fname.endswith(".zip"):
            try:
                with zipfile.ZipFile(fpath) as zf:
                    for info in zf.infolist():
                        # Skip directories
                        if info.is_dir():
                            continue
                        data = zf.read(info.filename)
                        inner_name = os.path.basename(info.filename)

                        try:
                            mol = _molecule_from_file(inner_name, data.decode())
                            results[fname].append((inner_name, mol))
                        except Exception as e:
                            errors.append(f"Error parsing molecule '{fname} : {inner_name}': {e}")

            except Exception as e:
                errors.append(f"Error decompressing ZIP archive '{fname}': {e}")

            continue

        # TAR archives (auto-detect compression)
        elif lower_fname.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
            try:
                # r:* autodetects compression (gz, bz2, xz) if available
                with tarfile.open(fpath, mode="r:*") as tf:
                    for member in tf.getmembers():
                        if not member.isfile():
                            continue
                        extracted = tf.extractfile(member)
                        if extracted is None:
                            continue
                        with extracted:
                            data = extracted.read()

                        inner_name = os.path.basename(member.name) or member.name

                        try:
                            mol = _molecule_from_file(inner_name, data.decode())
                            results[fname].append((inner_name, mol))
                        except Exception as e:
                            errors.append(f"Error parsing molecule '{fname} : {inner_name}': {e}")

            except Exception as e:
                errors.append(f"Error decompressing ZIP archive '{fname}': {e}")

            continue

        # Plain molecule file
        else:
            with open(fpath, "rt") as fh:
                data = fh.read()

                try:
                    mol = _molecule_from_file(os.path.basename(fpath), data)
                    results[fname].append((fname, mol))
                except Exception as e:
                    errors.append(f"Error parsing molecule '{fname}': {e}")

    return results, errors
