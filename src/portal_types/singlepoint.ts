import { BaseRecord, QCSpecification, Molecule } from "./common";

export type SinglepointRecord = BaseRecord & {
  record_type: "singlepoint";
  specification: QCSpecification;
  molecule_id: number;
  molecule?: Molecule;
  return_result?: unknown;
};
