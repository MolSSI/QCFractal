from __future__ import annotations

import os.path
import tempfile
from typing import List, Optional, Dict, Any, TYPE_CHECKING

import numpy as np
import qcelemental as qcel
import tabulate

from qcportal.dataset_models import load_dataset_view
from qcportal.manybody import ManybodyDatasetEntry, ManybodySpecification, ManybodyDataset, BSSECorrectionEnum
from qcportal.record_models import RecordStatusEnum

try:
    from crystalatte import build_nmer, cif_main, supercell2monomers
except ImportError:
    raise ImportError("Please install crystalatte to use this module")

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcportal.singlepoint import QCSpecification

nmer_names = ["monomers", "dimers", "trimers", "tetramers", "pentamers"]
nmer_name_map = {nmer_names[n - 1]: n for n in range(2, len(nmer_names) + 1)}


def _check_ds_complete(ds: ManybodyDataset, specification_name: str):
    if specification_name not in ds.specification_names:
        raise RuntimeError(f"Specification {specification_name} not found in dataset")

    stat = ds.status()
    if specification_name not in stat:
        raise RuntimeError(f"Specification {specification_name} found in dataset, but not submitted?")

    if set(stat[specification_name].keys()) != {RecordStatusEnum.complete}:
        raise RuntimeError(f"Specification {specification_name} not entirely complete for dataset {ds.name}")

    if stat[specification_name][RecordStatusEnum.complete] != len(ds.entry_names):
        raise RuntimeError(f"Not all entries submitted/completed")


def create_datasets(
    client: PortalClient,
    dataset_basename: str,
    cif_input_path: str,
    r_cut: List[float],
    qc_specification: QCSpecification,
    bsse_correction: List[BSSECorrectionEnum],
    manybody_keywords: Dict[str, Any],
    *,
    uniq_filter="ChSEV",
    r_cut_com=1000,
    bfs_thresh=1.2,
    cif_a=0,
    cif_b=0,
    cif_c=0,
    verbose: int = 1,
    tmpdir: Optional[str] = None,
):
    nmers_up_to = len(r_cut)
    nmer_cutoff = max(r_cut[1:])

    with tempfile.TemporaryDirectory(dir=tmpdir) as outdir:
        cif_name = os.path.split(cif_input_path)[1]
        cif_base = os.path.splitext(cif_name)[0]
        cif_output = os.path.join(outdir, f"{cif_base}.xyz")

        r_cut_monomer = cif_main(cif_input_path, cif_output, cif_a, cif_b, cif_c, r_cut[0], nmer_cutoff, True)
        nmers = supercell2monomers(cif_output, r_cut_monomer, bfs_thresh, verbose)

    total_monomers = len(nmers)

    # These are indexed by nmer count
    datasets: Dict[int, ManybodyDataset] = {}
    entries = {n: [] for n in range(2, nmers_up_to + 1)}

    #########################################
    # Create specifications for the datasets
    #########################################
    for n in range(2, nmers_up_to + 1):
        build_nmer(nmers, total_monomers, nmer_names[n - 1], r_cut[n - 1], r_cut_com, uniq_filter, verbose)
        datasets[n] = client.add_dataset("manybody", f"{dataset_basename}: {nmer_names[n - 1]}")

        mb_spec = ManybodySpecification(
            program="qcmanybody",
            levels={k: qc_specification for k in range(1, n + 1)},
            bsse_correction=bsse_correction,
            keywords=manybody_keywords,
        )

        datasets[n].add_specification("default", mb_spec)

    #########################################
    # Create entries for the datasets
    #########################################
    for keynmer, nmer in nmers.items():

        # Energies are not calculated for monomers. Rigid body approximation.
        if len(nmer["monomers"]) == 1:
            continue

        nat = nmer["coords"].shape[0]
        fidx = np.split(np.arange(nat), nmer["delimiters"])
        fragments = [fr.tolist() for fr in fidx if len(fr)]

        qcskmol = qcel.models.Molecule(
            symbols=nmer["elem"],
            geometry=nmer["coords"],
            fragments=fragments,
            fix_com=True,
            fix_orientation=True,
        )

        drop_attributes = ["elem", "coords"]
        attributes = {k: v for k, v in nmer.items() if k not in drop_attributes}

        ent = ManybodyDatasetEntry(name=keynmer, initial_molecule=qcskmol, attributes=attributes)

        n = len(nmer["monomers"])
        entries[n].append(ent)

    for n in range(2, nmers_up_to + 1):
        meta = datasets[n].add_entries(entries[n])
        if not meta.success:
            raise RuntimeError(f"Failed to add entries to dataset {datasets[n].name}. Error:\n {meta.error_string}")

    print("\n" + "-" * 80)
    for n, ds in datasets.items():
        print(f"Dataset {ds.name} [id {ds.id}] added with {len(entries[n])} entries")


def _analyze_datasets(specification_name, *datasets: ManybodyDataset):

    assert len(datasets) > 0

    ds_split_names = [ds.name.rsplit(":", maxsplit=1) for ds in datasets]

    # All have the same base
    dataset_basename = ds_split_names[0][0]
    assert all(x[0] == dataset_basename for x in ds_split_names)

    # for example, ds_map[2] = ds
    ds_map: Dict[int, ManybodyDataset] = {nmer_name_map[n.strip()]: ds for (_, n), ds in zip(ds_split_names, datasets)}

    nmer_results = []

    for n, ds in ds_map.items():
        for e, s, r in ds.iterate_records(specification_names=specification_name, status=RecordStatusEnum.complete):

            entry = ds.get_entry(e)
            assert len(entry.attributes["monomers"]) == n
            n_body_energy = r.properties["results"][f"cp_corrected_interaction_energy_through_{n}_body"]

            if n > 2:
                n_minus_1_body_energy = r.properties["results"][f"cp_corrected_interaction_energy_through_{n-1}_body"]
                nambe = n_body_energy - n_minus_1_body_energy
            else:
                nambe = n_body_energy

            contrib = nambe * entry.attributes["replicas"] / n

            rminseps = ""
            for r in sorted(entry.attributes["min_monomer_separations"]):
                rminseps += "{:6.3f} ".format(r * qcel.constants.bohr2angstroms)

            res = {
                "name": e,
                "nambe": nambe * qcel.constants.hartree2kcalmol * qcel.constants.cal2J,
                "replicas": entry.attributes["replicas"],
                "contrib": contrib * qcel.constants.hartree2kcalmol * qcel.constants.cal2J,
                "priority_cutoff": entry.attributes["priority_cutoff"],
                "rminseps": rminseps,
            }

            nmer_results.append(res)

    return nmer_results


def analyze_datasets(client: PortalClient, dataset_basename: str, specification_name: str):
    datasets: Dict[int, ManybodyDataset] = {}

    client_datasets = [x["dataset_name"] for x in client.list_datasets()]
    for n in range(2, len(nmer_names) + 1):
        dname = f"{dataset_basename}: {nmer_names[n - 1]}"
        if dname in client_datasets:
            datasets[n] = client.get_dataset("manybody", dname)

    if not datasets:
        raise RuntimeError(f"Could not find any datasets with prefix {dataset_basename}")

    print("Found datasets:")
    for n, ds in datasets.items():
        print(f"  [{nmer_names[n - 1]:<9}] {ds.name}")

    for ds in datasets.values():
        _check_ds_complete(ds, specification_name)

    return _analyze_datasets(specification_name, *datasets.values())


def analyze_dataset_views(specification_name: str, *view_paths: str):
    datasets = [load_dataset_view(x) for x in view_paths]
    return _analyze_datasets(specification_name, *datasets)


def results_summary_str(nmer_results):

    table_header = [
        "N-mer Name",
        "Non-Additive\nMB Energy\n(kJ/mol)",
        "Num.\nRep.",
        "N-mer\nContribution\n(kJ/mol)",
        "Partial\nCrystal\nLattice Energy\n(kJ/mol)",
        "Calculation\nPriority\n(Arb. Units)",
        "Minimum\nMonomer\nSeparations\n(A)",
    ]

    # Sort by priority cutoff (inverse)
    nmer_results = sorted(nmer_results, key=lambda x: -x["priority_cutoff"])

    crystal_lattice_energy = 0.0
    table_rows = []

    for nr in nmer_results:
        crystal_lattice_energy += nr["contrib"]
        table_rows.append(
            (
                nr["name"],
                nr["nambe"],
                nr["replicas"],
                nr["contrib"],
                crystal_lattice_energy,
                nr["priority_cutoff"],
                nr["rminseps"],
            )
        )

        # print(
        #    "{:26} | {:>12.8f} | {:>4} | {:>12.8f} | {:>13.8f} | {:12.6e} | {}".format(
        #        nr["name"],
        #        nr["nambe"],
        #        nr["replicas"],
        #        nr["contrib"],
        #        crystal_lattice_energy,
        #        nr["priority_cutoff"],
        #        nr["rminseps"],
        #    )
        # )

    output = tabulate.tabulate(
        table_rows, headers=table_header, tablefmt="simple", floatfmt=(".8f", ".8f", ".8f", ".4e")
    )

    output += "\n\n"
    output += f"Crystal Lattice Energy (kJ/mol)   = {crystal_lattice_energy:9.8f}\n"
    output += f"Crystal Lattice Energy (kcal/mol) = {crystal_lattice_energy / qcel.constants.cal2J:9.8f}\n"
    return output


def print_results_summary(nmer_results):
    print(results_summary_str(nmer_results))
