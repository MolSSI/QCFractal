import { CalculationRecord, Molecule } from "./PortalTypes";

export const parseToDate = (isoString?: string): Date | undefined => {
  if (!isoString) {
    return undefined;
  }

  // Trim to milliseconds (3 digits after the dot)
  const trimmed = isoString.replace(/(\.\d{3})\d+/, "$1");
  return new Date(trimmed);
};

export const getRecordReprMolecule = (record: CalculationRecord): Molecule => {
  if (record.record_type == "singlepoint") return record.molecule;
  if (record.record_type == "optimization") return record.initial_molecule;
  if (record.record_type == "torsiondrive") return record.initial_molecules[0];
  if (record.record_type == "gridoptimization") return record.initial_molecule;
  if (record.record_type == "manybody") return record.initial_molecule;
  if (record.record_type == "neb") return record.initial_chain[0];

  throw new Error(`Unknown or unhandled record type: ${record.record_type}`);
};
