# mongo_qcdb
MongoDB backend for storing quantum chemical databases

# Introduction
mongo_qcdb is a MongoDB database backend for quantum chemical activities, particularly for use with the Psi4 project (). Currently, there is no open, standardized, and centralized repository for the outputs of computational chemistry calculations. Ease of access to such data is essential to progress, as individual calculations can take days or weeks to run. This backend is hopefully the answer to that problem.

# Schema Guide
This MongoDB database has 3 collections: `databases`, `molecules`, and `pages`.

## molecules
A collection of atomic documents. That is, they do not have an external references and essentially define a set of usable data units. The schema of a database document is described below in JSON.

```json
{
  "name": "Carbon Dioxide",
  "symbol": "CO2",
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

## databases
Collection which is home to a number of database documents. The schema of a database document is described below in JSON

```json
{
  "reactions": [
    {
      "molecules": [4192705914, 4192705918],
      "coefficients": [1.0, 1.2]
    },
    {
      "molecules": [4141775914, 4110025918],
      "coefficients": [2.0, 5.4]
    },
    {
      "molecules": [4192401914, 4444165918],
      "coefficients": [3.0, 6.4]
    }
  ],
  "citation": "A. Smith and B. Jones",
  "link": "http://example.com"
}
```

Each entry in the `molecules` array is a manual reference to a molecule `_id`. For more information on manual references, see https://docs.mongodb.com/v3.2/reference/database-references/#document-references

##pages
A collection of `page` documents, which is essentially a dual key to multiple value lookup entry. Each page is a separate entry. The key values are [`molecule`,`method`].

```json
{
  "molecule": 4129481723,
  "method": "METHOD_A",
  "value_1": 123,
  "value_2": 234,
  "value_3": 345,
  "citation": "A. Smith and B. Jones",
  "link": "http://example.com"
}
```
Again, molecule is the `_id` of the refrenced molecule. `value_n` will be replaced with proper values eventually, they are just placeholders.

