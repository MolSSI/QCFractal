# mongo_qcdb
MongoDB backend for storing quantum chemical databases

# Introduction
mongo_qcdb is a MongoDB database backend for quantum chemical activities, particularly for use with the Psi4 project (http://www.psicode.org/ or https://github.com/psi4). Currently, there is no open, standardized, and centralized repository for the outputs of computational chemistry calculations. Ease of access to such data is essential to progress, as individual calculations can take days or weeks to run. This backend is hopefully the answer to that problem.

# Schema Guide
This MongoDB database has 3 collections: `databases`, `molecules`, and `pages`.

## hashing and uniqueness
Before any document is entered into the Mongo database, we compute a SHA1 hash based on its JSON. This hash is used as the `_id` of the document instead of MongoDB's default ObjectID. SHA1 hashes are superior to ObjectIDs because the SHA1 hash is persistent through database flushes, whereas an ObjectID would be reset if a document is removed and re-added.

### molecules
A collection of atomic documents. That is, they do not have an external references and essentially define a set of usable data units. The schema of a database document is described below in JSON.

```json
{
  "symbols": ["C", "O", "O"],
  "masses": [16.0, 18.0, 18.0],
  "name": "Carbon Dioxide",
  "charge": 0.0,
  "multiplicity": 1,
  "ghost": [false, false, false],
  "comment": "A test comment",
  "geometry": [
    [
      3.11,
      5.12,
      6.14
    ],
    [
      -3.13,
      -7.12,
      -9.18
    ],
    [
      1.22,
      5.11,
      -1.89
    ]
  ],
  "provenance": {
    "doi": "val",
    "tag": "val",
    "version": "0.7.4alpha0+21.gd658905.dirty",
    "routine": "moldesign.from_smiles",
    "creator": "MolecularDesignToolkit"
  }
}
```

### databases
Collection which is home to a number of database documents. The schema of a database document is described below in JSON

```json
{
  "name": "S22",
  "reactions": [
    {
      "name": "cool reaction",
      "molecules": ["8e102b34c4441c4b164a7d678591df550c90de74", "dbbacd78247e7b39ee5cb8e78d74423e98639203"],
      "coefficients": [1.0, 1.2],
      "methods": ["MP2/aug-cc-pVDZ", "MP3/byg-aa-pPAZ", "N92/ygk-eq-hONE"],
      "subset": "string",
      "attributes": {
        "R": 1.0,
        "Q": 2.0
      }
    },
    {
      "name": "crazy reaction",
      "molecules": ["8e102b34c4441c4b164a7d678591df550c90de74", "8e102b34c4441c4b164a7d678591df550c90de74"],
      "coefficients": [2.0, 5.4],
      "methods": ["MP2/aug-cc-pVDZ", "MP3/byg-aa-pPAZ", "N92/ygk-eq-hONE"],
      "subset": "string2",
      "attributes": {
        "R": 30,
        "Q": 4.0
      }
    }
  ],
  "citation": "A. Smith and B. Jones",
  "link": "http://example.com"
}
```

Each entry in the `molecules` array is the `_id` of a molecule known in the `molecules` collection. This is known as a manual reference: https://docs.mongodb.com/v3.2/reference/database-references/#manual-references

###pages
A collection of `page` documents, which is essentially a dual key to multiple value lookup entry. Each page is a separate entry. The keys needed to access a page are [`molecule`,`method`].

```json
{
  "molecule": "dbbacd78247e7b39ee5cb8e78d74423e98639203",
  "method": "A",
  "value": [1.34],
  "type": "gradient",
  "options": {
    "opt1": "val",
    "opt2": "val"
  },
  "variables": {
    "var1": "val",
    "var2": "val"
  },
  "provenance": {
    "doi": "val",
    "tag": "val",
    "version": "0.7.4alpha0+21.gd658905.dirty",
    "routine": "moldesign.from_smiles",
    "creator": "MolecularDesignToolkit"
  }
}
```
Again, molecule is the `_id` of the referenced molecule. `value_n` will be replaced with proper values eventually, they are just placeholders. Again, a manual reference.
