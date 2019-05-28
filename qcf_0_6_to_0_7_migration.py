import pymongo
import tqdm
import qcelemental as qcel
import qcfractal.interface as ptl

client = pymongo.MongoClient("localhost:27017")
client.drop_database("qcarchive2")
client.admin.command("copydb", fromdb="qcarchive", todb="qcarchive2")

db = client.qcarchive2

print("\nUpdating Molecule hashes:")
mol_query = db.molecule.find(limit=1000)
total = mol_query.count()

updates = 0

# First update molecules
for raw_mol in tqdm.tqdm(mol_query, total=total):

    tmpdata = raw_mol.copy()
    tmpdata.pop("_id", None)
    tmpdata.pop("molecular_formula", None)
    tmpdata.pop("molecule_hash", None)

    conupd = False
    if ("connectivity" in tmpdata) and (tmpdata["connectivity"] == []):
        tmpdata["connectivity"] = None
        conupd = True
    
    mol = qcel.models.Molecule(**tmpdata, validate=False)

    mhash = mol.get_hash()
    if mhash == raw_mol["molecule_hash"]:
        if conupd:
            print(raw_mol)
            raise Exception()
        continue 


    upd_data = {"molecule_hash": mhash, "identifiers": tmpdata["identifiers"]}
    upd_data["identifiers"]["molecule_hash"] = mhash
    if conupd:
        upd_data["connectivity"] = None

    upd = db.molecule.update_one({"_id": raw_mol["_id"]}, {"$set": upd_data}) 
    if upd.modified_count != 1:
        print(upd)
        print(upd_data)
        break
    else:
        updates += 1

print(f"Total number of result updates: {updates}")


## Result updates
print("\nUpdating Result Fields:")

upd = db.result.update_many({}, {"$rename": {"extras.local_qcvars": "extras.qcvars", "properties.mp2_total_correlation_energy": "properties.mp2_correlation_energy"}})
print(f"Total number of result updates: {upd.modified_count}")

## Procedure updates

## Result updates
print("\nUpdating TorsionDrive Hashes:")

td_query = db.procedure.find({"procedure": "torsiondrive"}, limit=1000)
total = td_query.count()

updates = 0

# First update molecules
for raw_td in tqdm.tqdm(td_query, total=total):

    tmpdata = raw_td.copy()
    tmpdata.pop("_id")
    tmpdata.pop("_cls")
    tmpdata.pop("hash_index")
    td = ptl.models.TorsionDriveRecord(**tmpdata)

    upd = db.molecule.update_one({"_id": raw_td["_id"]}, {"$set": {"hash_index": td.hash_index}}) 
    if upd.modified_count != 1:
        print(upd)
        print(upd_data)
        break
    else:
        updates += 1

print("\nUpdating TorsionDriveDataset Fields:")

td_query = db.collection.find({"collection": "torsiondrivedataset"}, limit=1000)

for raw_col in td_query:

    new_records = {}
    for key, record in raw_col["records"].items():
        tmp = record.copy()
        tmp["object_map"] = tmp.pop("torsiondrives")
        new_records[key] = tmp

    db.collection.update_one({"_id": raw_col["_id"]}, {"records": new_records})
    

