# mongo_qcdb
MongoDB backend for storing quantum chemical databases

At the highest level, mongo_qcdb is an environment from which you can run PSI4 computations. Parameters to these computations can be accessed from the underlying persistent database, and the results of these computations are dumped into the same database after completion. This opens the door for centralized computing in computational chemistry. mongo_qcdb is a python package.


Now, with less abstraction. mongo_qcdb manages accesses and stores of PSI4 input and output data to and from an underlying Mongo database. It follows a client-server paradigm in which a client makes GET or POST requests to a remote qcdb_server to affect or access the database. These client functions wrap advanced queries which are useful to a computational chemistry researcher. Clients can also submit PSI4 jobs to a distributed computing cluster using Dask, so that users can turn off their computers as computations run on the server and have their results waiting in the database after completion.


When using mongo_qcdb’s functions, clients must define a “project” in which they want to work by providing project name as a parameter. Project workspaces allow for researchers to isolate their work on a centralized system and to provide temporary environments for testing new ideas. For example, all of a server’s data can be stored in a “master” project, and clients can use their functions to copy this data to their own project for manipulation and experimenting. 

# Introduction
mongo_qcdb is a MongoDB database backend for quantum chemical activities, particularly for use with the Psi4 project (http://www.psicode.org/ or https://github.com/psi4). Currently, there is no open, standardized, and centralized repository for the outputs of computational chemistry calculations. Ease of access to such data is essential to progress, as individual calculations can take days or weeks to run. This backend is hopefully the answer to that problem.

# Schema Guide
This MongoDB database has 3 collections: `databases`, `molecules`, and `pages`. All documents are imported from JSON files.

## Hashing and Uniqueness

### Technique

Before any document is entered into the Mongo database, we compute a SHA1 hash based on its JSON. This hash is used as the `_id` of the document instead of MongoDB's default ObjectID. This unique ID attached to each document added to the database prevents duplicate entries and increases access speed dramatically. To understand the benefits of indexing, see https://docs.mongodb.com/v3.2/core/index-single/

SHA1 hashes are superior to ObjectIDs because the SHA1 hash is reflective of the actual content of the document. Hence, it is persistent through database flushes, whereas an ObjectID would be reset if a document is removed and re-added.

### Hashed Fields

For all document types, we only take a hash of a few essential fields as opposed to the entire document. This allows for small changes to the JSON during production without the need to recalculate the entire hash.

Hashed fields for `molecules`
```json
"symbols": ["C", "O", "O"],
"masses": [16.0, 18.0, 18.0],
"name": "Carbon Dioxide",
"charge": 0.0,
"multiplicity": 1,
"real": [false, false, false],
"geometry": [ .. ],
"fragments":[[ .. ], ..],
"fragment_charges": [3.1, 2.1, 1.1],
"fragment_multiplicities": [1, 2, 3]
```

Hashed fields for `databases`
```json
"name": "S22"
```

Hashed fields for `pages`
```json
"molecule": "dbbacd78247e7b39ee5cb8e78d74423e98639203",
"method": "MP2/aug-cc-pVDZ"
```

### Why not use unique indexing to manage duplicates?
MongoDB does not support indexing of dictionary fields like `geometry`. Computing a SHA1 hash of the fields that define uniqueness is a much cleaner solution.

## JSON Structure

### molecules
A collection of atomic documents. That is, they do not have an external references and essentially define a set of usable data units. The schema of a database document is described below in JSON.

```json
{
  "symbols": ["C", "O", "O"],
  "masses": [16.0, 18.0, 18.0],
  "name": "Carbon Dioxide",
  "charge": 0.0,
  "multiplicity": 1,
  "real": [false, false, false],
  "comment": "A test comment",
  "geometry": [
    [3.11, 5.12, 6.14],
    [-3.13, -7.12, -9.18],
    [1.22, 5.11, -1.89]
  ],
  "fragments": [
    [1, 2, 3],
    [4, 5, 6],
    [7, 8, 9]
  ],
  "fragment_charges": [3.0, 2.2, 4.1],
  "fragment_multiplicities": [5, 2, 3],
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
      "stoichiometry": {
        "default": {
          "mol1hash": 1,
          "mol2hash": -1,
          "mol3hash":-1
        },
        "cp": {
          "mol1hash": 1,
          "mol2Mhash": -1,
          "mol3Mhash": -1
        },
        "sapt": {
          "mol1hash": 1
        }
      },
      "reaction_results": ["MP2/aug-cc-pVDZ", "MP3/byg-aa-pPAZ", "N92/ygk-eq-hONE"],
      "subset": "string",
      "attributes": {
        "R": 1.0,
        "Q": 2.0
      }
    },
    {
      "name": "crazy reaction",
      "stoichiometry": {
        "default": {
          "mol1hash": 1,
          "mol2hash": -1,
          "mol3hash":-1
        },
        "cp": {
          "mol1hash": 1,
          "mol2Mhash": -1,
          "mol3Mhash": -1
        },
        "sapt": {
          "mol1hash": 1
        }
      },
      "reaction_results": ["MP2/aug-cc-pVDZ", "MP3/byg-aa-pPAZ", "N92/ygk-eq-hONE"],
      "subset": "string2",
      "attributes": {
        "R": 30,
        "Q": 4.0
      }
    }
  ],
  "provenance": {
    "doi": "val",
    "tag": "val",
    "version": "0.7.4alpha0+21.gd658905.dirty",
    "routine": "moldesign.from_smiles",
    "creator": "MolecularDesignToolkit"
  },
  "citation": "A. Smith and B. Jones",
  "link": "http://example.com"
}
```

Each entry in the `molecules` array is the `_id` of a molecule known in the `molecules` collection. This is known as a manual reference: https://docs.mongodb.com/v3.2/reference/database-references/#manual-references

### pages
A collection of `page` documents, which is essentially a dual key to multiple value lookup entry. Each page is a separate entry. The keys needed to access a page are [`molecule`,`method`].

```json
{
  "molecule_hash": "dbbacd78247e7b39ee5cb8e78d74423e98639203",
  "modelchem": "MP2/aug-cc-pVDZ",
  "return_value": 1.34,
  "type": "energy",
  "success": true,
  "error": "",
  "raw_output": "",
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
Again, molecule is the `_id` of the referenced molecule. Again, a manual reference.


### testing
To setup testing run the following:

    - mongod
    - pip install -e . (only needed once!)
    - py.test -v
