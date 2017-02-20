"""A collection of fields which determine the fields that we hash, or are valid
"""

import copy

### Hash Fields
hash_fields = {}
hash_fields["molecule"] = (
    "symbols", "masses", "name", "charge", "multiplicity", "real", "geometry", "fragments",
    "fragment_charges", "fragment_multiplicities"
)
hash_fields["database"] = ("name", )
hash_fields["page"] = ("modelchem", "molecule_hash")

### Valid Fields
valid_fields = {}
valid_fields["molecule"] = copy.deepcopy(hash_fields["molecule"])
valid_fields["molecule"] = valid_fields["molecule"] + ("provenance", "comment")

valid_fields["database"] = copy.deepcopy(hash_fields["database"])
valid_fields["database"] = valid_fields["database"] + ("rxn_type", "provenance")

valid_fields["page"] = copy.deepcopy(hash_fields["page"]) + ("provenance", )
