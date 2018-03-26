"""A collection of fields which determine the fields that we hash, or are valid
"""

import copy
import hashlib
import json

### Hash Fields
hash_fields = {}
hash_fields["molecule"] = ("symbols", "masses", "charge", "multiplicity", "real", "geometry",
                           "fragments", "fragment_charges", "fragment_multiplicities")
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
    if (field_type == "molecules"):
        field_type = "molecule"
    elif (field_type == "databases"):
        field_type = "database"
    elif (field_type == "pages"):
        field_type = "page"
    m = hashlib.sha1()
    concat = ""
    if field_type is None:
        return hash(str(data))

    for field in hash_fields[field_type]:
        concat += json.dumps(data[field])
    m.update(concat.encode("utf-8"))
    sha1 = m.hexdigest()
    return sha1
