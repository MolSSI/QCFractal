"""A collection of fields which determine the fields that we hash, or are valid
"""

import copy
import hashlib
import json

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



def get_hash(data, field_type):
    m = hashlib.sha1()
    concat = ""
    for field in hash_fields[field_type]:
        concat += json.dumps(data[field])
    m.update(concat.encode("utf-8"))
    sha1 = m.hexdigest()
    return sha1