"""
Tests the QCPortal dataset object
"""

import pytest

from . import portal
from . import test_helper as th


def _compare_stoichs(stoich, stoich_other):
    mols = list(stoich)
    mols_other = list(stoich_other)
    assert th.compare_lists(mols, mols_other)

    for mol in mols:
        assert stoich[mol] == stoich_other[mol]

    return True


def _compare_rxn_stoichs(ref, new):
    stoich = ref.stoichiometry
    stoich_other = new.stoichiometry

    keys = list(stoich)
    keys_other = list(stoich_other)
    assert th.compare_lists(keys, keys_other)

    for k in keys:
        _compare_stoichs(stoich[k], stoich_other[k])

    return True


# Build an interesting dataset
@pytest.fixture
def water_ds():
    # Create water ReactionDataset, also tests that ds_type is case insensitive
    ds = portal.collections.ReactionDataset("Water Data", ds_type="RxN")

    # Build the water dimer.
    dimer = portal.data.get_molecule("water_dimer_minima.psimol")
    frag_0 = dimer.get_fragment(0, orient=True, group_fragments=True)
    frag_1 = dimer.get_fragment(1, orient=True, group_fragments=True)
    frag_0_1 = dimer.get_fragment(0, 1, orient=True, group_fragments=True)
    frag_1_0 = dimer.get_fragment(1, 0, orient=True, group_fragments=True)

    # Add single stoich rxn via list
    ds.add_rxn(
        "Water Dimer, nocp",
        [(dimer, 1.0), (frag_0, -1.0), (frag_1, -1.0)],
        attributes={"R": "Minima"},
        reaction_results={"Benchmark": -20.0, "DFT": -10.0},
    )

    dimer_string = dimer.to_string("psi4")
    # Add single stoich from strings, not a valid set
    ds.add_rxn(
        "Water Dimer, dimer - str (invalid)",
        [(dimer_string, 1.0), (frag_0, 0.0)],
        attributes={"R": "Minima"},
        reaction_results={"Benchmark": -20.0, "DFT": -10.0},
    )

    # Add single stoich rxn via hashes
    ds.add_rxn(
        "Water Dimer, nocp - hash",
        [(dimer.get_hash(), 1.0), (frag_0.get_hash(), -1.0), (frag_1.get_hash(), -1.0)],
        attributes={"R": "Minima"},
        reaction_results={"Benchmark": -5.0},
    )

    # Add multi stoich reaction via dict
    with pytest.raises(KeyError):
        ds.add_rxn("Null", {"Null": [(dimer, 1.0)]})

    # nocp and cp water dimer
    ds.add_rxn(
        "Water Dimer, all",
        {
            "cp": [(dimer, 1.0), (frag_0_1, -1.0), (frag_1_0, -1.0)],
            "default": [(dimer, 1.0), (frag_0, -1.0), (frag_1, -1.0)],
        },
        other_fields={"Something": "Other thing"},
    )

    ds.add_ie_rxn("Water dimer", dimer.to_string("psi4"))

    # Add unverified records (requires a active server)
    ds.data.__dict__["records"] = ds._new_records

    return ds


# Build a nbody dataset
@pytest.fixture
def nbody_ds():
    ds = portal.collections.ReactionDataset("N-Body Data")

    dimer = portal.data.get_molecule("water_dimer_minima.psimol")
    frag_0 = dimer.get_fragment(0, orient=True, group_fragments=True)
    frag_1 = dimer.get_fragment(1, orient=True, group_fragments=True)
    frag_0_1 = dimer.get_fragment(0, 1, orient=True, group_fragments=True)
    frag_1_0 = dimer.get_fragment(1, 0, orient=True, group_fragments=True)

    ds.add_rxn(
        "Water Dimer, bench",
        {
            "cp1": [(frag_0_1, 1.0), (frag_1_0, 1.0)],
            "default1": [(frag_0, 1.0), (frag_1, 1.0)],
            "cp": [(dimer, 1.0)],
            "default": [(dimer, 1.0)],
        },
    )

    ds.add_ie_rxn("Water Dimer", dimer.to_string("psi4"))
    ds.add_ie_rxn("Ne Tetramer", portal.data.get_molecule("neon_tetramer.psimol"))

    # Ne Tetramer benchmark
    ds.ne_stoich = {
        "name": "Ne Tetramer",
        "stoichiometry": {
            "default1": {"acfce0c62cac57640d221a545998711853606bc9": 4.0},
            "cp1": {
                "532ae1b0fab346aeaa2167972b5b61079081209f": 1.0,
                "f89a033c52c527b0a85f271193f3515b0aec190b": 1.0,
                "43fafa29d4d3197738e741051aff71ffe98264b3": 1.0,
                "cf36366bf4d5a2f96cf0669425c49bd929d7f081": 1.0,
            },
            "default2": {
                "acfce0c62cac57640d221a545998711853606bc9": -8.0,
                "e643e8f4e8668793165339e98f4f314cdac6e909": 1.0,
                "0768204c7ddc575a77574b49598b89eb04d4522b": 1.0,
                "05cb2410011128a45b5c3479e7724c60b91bfbee": 2.0,
                "444dba64349a093f8da2c5c6594b128f54619d92": 1.0,
                "75608da1aad0be8f353cab54e2674a138c5d04e7": 1.0,
            },
            "cp2": {
                "532ae1b0fab346aeaa2167972b5b61079081209f": -2.0,
                "f89a033c52c527b0a85f271193f3515b0aec190b": -2.0,
                "43fafa29d4d3197738e741051aff71ffe98264b3": -2.0,
                "cf36366bf4d5a2f96cf0669425c49bd929d7f081": -2.0,
                "f08a3ea572f4a371c18b99be63545a23f9cf17d2": 1.0,
                "eb6efb5f6a7238c0582517eeba0c5b940d84c51d": 1.0,
                "75a23e5e6f995aaee9a049226701f543f0ee9a82": 1.0,
                "8d5280d9aa82243a12b319b24bd19506bae77853": 1.0,
                "b7f73f0957f1f14469dde2000605061fbbe4939e": 1.0,
                "bee9dfe443c016283ef2a4d98959a736b0d0b591": 1.0,
            },
            "default3": {
                "acfce0c62cac57640d221a545998711853606bc9": 4.0,
                "e643e8f4e8668793165339e98f4f314cdac6e909": -1.0,
                "0768204c7ddc575a77574b49598b89eb04d4522b": -1.0,
                "05cb2410011128a45b5c3479e7724c60b91bfbee": -2.0,
                "444dba64349a093f8da2c5c6594b128f54619d92": -1.0,
                "75608da1aad0be8f353cab54e2674a138c5d04e7": -1.0,
                "da4d287f648d33203652cd5a8fc621e331c16fab": 1.0,
                "d7914b7213eff227af8228f64c26187eec343c21": 1.0,
                "efcfd9e9ae9f33dcfb2780367690dcfff8bcb397": 1.0,
                "4e3e461ab1439dc360a4b001ac885a8240b27640": 1.0,
            },
            "cp3": {
                "532ae1b0fab346aeaa2167972b5b61079081209f": 1.0,
                "f89a033c52c527b0a85f271193f3515b0aec190b": 1.0,
                "43fafa29d4d3197738e741051aff71ffe98264b3": 1.0,
                "cf36366bf4d5a2f96cf0669425c49bd929d7f081": 1.0,
                "f08a3ea572f4a371c18b99be63545a23f9cf17d2": -1.0,
                "eb6efb5f6a7238c0582517eeba0c5b940d84c51d": -1.0,
                "75a23e5e6f995aaee9a049226701f543f0ee9a82": -1.0,
                "8d5280d9aa82243a12b319b24bd19506bae77853": -1.0,
                "b7f73f0957f1f14469dde2000605061fbbe4939e": -1.0,
                "bee9dfe443c016283ef2a4d98959a736b0d0b591": -1.0,
                "32b290f24926d5ce6ca7ed4eb31d034bcc18a784": 1.0,
                "214a0c4469f1ea514e0ee7a256dc601f39a90498": 1.0,
                "fb3eeb93515fac8bb807af999bddd883c27af7fa": 1.0,
                "3e4f560822d53d4fd53520d354eb4fa6632c745a": 1.0,
            },
            "default": {"c0e6a8225f1eb3ab5f7e81a755a91dd800869f23": 1.0},
            "cp": {"c0e6a8225f1eb3ab5f7e81a755a91dd800869f23": 1.0},
        },
        "attributes": {},
        "reaction_results": {"default": {}},
    }

    # Add unverified records (requires a active server)
    ds.data.__dict__["records"] = ds._new_records

    return ds


# Test conventional add
def test_rxn_add(water_ds):

    assert water_ds.data.name == "Water Data"
    assert len(water_ds.get_index()) == 5

    nocp_stoich_class = water_ds.get_rxn("Water Dimer, nocp").stoichiometry["default"]
    nocp_stoich_hash = water_ds.get_rxn("Water Dimer, nocp - hash").stoichiometry["default"]
    nocp_stoich_dict = water_ds.get_rxn("Water Dimer, all").stoichiometry["default"]

    # Check if both builds check out
    _compare_stoichs(nocp_stoich_class, nocp_stoich_hash)
    _compare_stoichs(nocp_stoich_class, nocp_stoich_dict)


# Test IE add
def test_nbody_rxn(nbody_ds):

    # Check the Water Dimer
    water_stoich_bench = nbody_ds.get_rxn("Water Dimer, bench")
    water_stoich = nbody_ds.get_rxn("Water Dimer")
    _compare_rxn_stoichs(water_stoich, water_stoich_bench)

    bench_vals = {"default1": 1, "cp1": 4, "default2": 6, "cp2": 10, "default3": 10, "cp3": 14, "default": 1, "cp": 1}
    # Check some basics
    for key in list(nbody_ds.ne_stoich["stoichiometry"]):
        assert bench_vals[key] == len(nbody_ds.ne_stoich["stoichiometry"][key])

    # Check the N-body
    ne_stoich = nbody_ds.get_rxn("Ne Tetramer")
    mh = list(ne_stoich.stoichiometry["default"])[0]
    # print(ne_stoich)
    # _compare_rxn_stoichs(nbody_ds.ne_stoich, ne_stoich)


def test_database_history():
    ds = portal.collections.Dataset("history_test")
    history = [
        ("energy", "p3", "m1", "b2", "o1"),
        ("energy", "p1", "m1", None, "o1"),
        ("energy", "p1", "m1", None, "o2"),
        ("energy", "p1", "m2", "b3", "o1"),
        ("gradient", "p1", "m2", None, None),
    ]  # yapf: disable

    for h in history:
        ds._add_history(driver=h[0], program=h[1], method=h[2], basis=h[3], keywords=h[4])

    assert ds.list_records().shape[0] == 5
    assert ds.list_records(program="P1").shape[0] == 4
    assert ds.list_records(basis="None").shape[0] == 3
    assert ds.list_records(keywords="None").shape[0] == 1
