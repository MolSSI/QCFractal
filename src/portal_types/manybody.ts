import { BaseRecord, QCSpecification, Molecule } from "./common";

export type ManybodySpecification = {
  program: string;
  keywords: Record<string, unknown>;
  singlepoint_specification?: QCSpecification;
};

export type ManybodyRecord = BaseRecord & {
  record_type: "manybody";
  specification: ManybodySpecification;
  initial_molecule_id: number;
  initial_molecule?: Molecule;
  cluster_records?: Record<string, unknown>[];
};
