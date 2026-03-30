import { BaseRecord, QCSpecification, Molecule } from "./common";
import { SinglepointRecord } from "./singlepoint";

export type OptimizationSpecification = {
  program: string;
  keywords: Record<string, unknown>;
  protocols: Record<string, unknown>;
  qc_specification: QCSpecification;
};

export type OptimizationRecord = BaseRecord & {
  record_type: "optimization";
  specification: OptimizationSpecification;
  initial_molecule_id: number;
  initial_molecule?: Molecule;
  final_molecule_id?: number;
  final_molecule?: Molecule;
  energies?: number[];
  trajectory?: SinglepointRecord[];
};
