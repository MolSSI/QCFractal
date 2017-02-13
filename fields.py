

# What fields do we hash?
hash_fields = {}
hash_fields["molecule"] = ["symbols", "masses", "name", "charge", "multiplicity", "real", "geometry", "fragments",
                            "fragment_charges", "fragment_multiplicities"]
hash_fields["database"] = ["name"]
hash_fields["page"] = ["modelchem", "molecule_hash"]

# What fields are valid?
valid_fields = {k : v[:] for k, v in hash_fields.items()}

valid_fields["molecule"].extend(["provenance", "comment"])
