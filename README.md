# mongo_qcdb
MongoDB backend for storing quantum chemical databases

# Introduction
mongo_qcdb is a MongoDB database backend for quantum chemical activities, particularly for use with the Psi4 project (http://www.psicode.org/ or https://github.com/psi4). Currently, there is no open, standardized, and centralized repository for the outputs of computational chemistry calculations. Ease of access to such data is essential to progress, as individual calculations can take days or weeks to run. This backend is hopefully the answer to that problem.

# Schema Guide
This MongoDB database has 3 collections: `databases`, `molecules`, and `pages`.

### molecules
A collection of atomic documents. That is, they do not have an external references and essentially define a set of usable data units. The schema of a database document is described below in JSON.

```json
{
  "symbol": "CO2",
  "name": "Carbon Dioxide",
  "geometry": [
    {
      "x": "3.11",
      "y": "5.12",
      "z": "6.14"
    },
    {
      "x": "-3.13",
      "y": "-7.12",
      "z": "-9.18"
    },
    {
      "x": "1.22",
      "y": "5.11",
      "z": "-1.89"
    }
  ]
}
```
On initialization, the `molecules` collection is given a unique index on the `symbol` field. This is to enable rapid lookup from queries and obviates the need for manual references to the `_id` field. To understand the efficiency gains of an index, see https://docs.mongodb.com/manual/indexes/

Because this is a unique index, each entry in the `molecules` database must have a unique `symbol`. To understand what this means, see https://docs.mongodb.com/v3.0/core/index-unique/

### databases
Collection which is home to a number of database documents. The schema of a database document is described below in JSON

```json
{
  "reactions": [
    "name": "S22",
    {
      "molecules": ["CO2", "C6H12O6"],
      "coefficients": [1.0, 1.2]
    },
    {
      "molecules": ["CO2", "H2O"],
      "coefficients": [2.0, 5.4]
    },
    {
      "molecules": ["H2O", "C6H12O6"],
      "coefficients": [3.0, 6.4]
    }
  ],
  "citation": "A. Smith and B. Jones",
  "link": "http://example.com"
}
```

Each entry in the `molecules` array is the symbol of a molecule known in the `molecules` collection.

###pages
A collection of `page` documents, which is essentially a dual key to multiple value lookup entry. Each page is a separate entry. The keys needed to access a page are [`molecule`,`method`].

```json
{
  "molecule": "CO2",
  "method": "METHOD_A",
  "value_1": 123,
  "value_2": 234,
  "value_3": 345,
  "citation": "A. Smith and B. Jones",
  "link": "http://example.com"
}
```
Again, molecule is the `symbol` of the refrenced molecule. `value_n` will be replaced with proper values eventually, they are just placeholders.

On initialization, the `pages` collection is given a unique compound index on the `molecule` and `method` fields. Again, this is a unique compound key, so you can only have one of each pair. To understand how compound indices differ from regular indices, see https://docs.mongodb.com/manual/core/index-compound/#index-type-compound
