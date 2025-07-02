import { RecordData, Molecule } from "./PortalTypes";

export const parseToDate = (isoString?: string): Date | undefined => {
  if (!isoString) {
    return undefined;
  }

  // Trim to milliseconds (3 digits after the dot)
  const trimmed = isoString.replace(/(\.\d{3})\d+/, "$1");
  return new Date(trimmed);
};

export const getRecordReprMolecule = (record: RecordData): Molecule => {
  if (record.record_type == "singlepoint") return record.molecule_id;
  if (record.record_type == "optimization") return record.initial_molecule_id;
  if (record.record_type == "torsiondrive")
    return record.initial_molecules_id[0];
  if (record.record_type == "gridoptimization")
    return record.initial_molecule_id;
  if (record.record_type == "manybody") return record.initial_molecule_id;
  if (record.record_type == "neb") return record.initial_chain[0];
  if (record.record_type === "reaction") return undefined;

  throw new Error(`Unknown or unhandled record type: ${record.record_type}`);
};
