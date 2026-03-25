import { RecordData, Molecule } from "./PortalTypes";

export const parseToDate = (isoString?: string): Date | undefined => {
  if (!isoString) {
    return undefined;
  }

  // Trim to milliseconds (3 digits after the dot)
  const trimmed = isoString.replace(/(\.\d{3})\d+/, "$1");
  return new Date(trimmed);
};

export const getRecordReprMolecule = (record: RecordData): Molecule | undefined => {
  if (record.record_type == "singlepoint") return record.molecule_id;
  if (record.record_type == "optimization") return record.initial_molecule_id;
  if (record.record_type == "torsiondrive")
    return record.initial_molecules_id![0];
  if (record.record_type == "gridoptimization")
    return record.initial_molecule_id;
  if (record.record_type == "manybody") return record.initial_molecule_id;
  if (record.record_type == "neb") return record.initial_chain![0];
  if (record.record_type === "reaction") return undefined;

  throw new Error(`Unknown or unhandled record type: ${record.record_type}`);
};


export const updateFavoritesList = (existing_favorites: number[] | undefined, proj_id: number): number[] => {
  // adds or removes the new_id to/from the existing_favorites
  // Also handles if existing favorites is undefined
  if (!existing_favorites) return [proj_id];

  // If the project is already in the list, remove it
  if (existing_favorites.includes(proj_id)) {
    return existing_favorites.filter((id) => id !== proj_id);
  }
  return [...existing_favorites, proj_id];
};