import { BaseRecord, QCSpecification, Molecule } from "./common";

export type SinglepointRecord = BaseRecord & {
  record_type: "singlepoint";
  specification: QCSpecification;
  molecule_id: number;
  molecule?: Molecule;
  return_result?: unknown;
};

export type SinglepointDatasetSpecification = {
  name: string;
  specification: QCSpecification;
  description?: string;
};

export type SinglepointDatasetEntry = {
  name: string;
  molecule: Molecule;
  additional_keywords: Record<string, unknown>;
  attributes: Record<string, unknown>;
  comment?: string;
  local_results?: Record<string, unknown>;
};
