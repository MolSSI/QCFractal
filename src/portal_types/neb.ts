import { BaseRecord, QCSpecification, Molecule } from "./common";
import { OptimizationSpecification } from "./optimization";

export type NEBSpecification = {
  program: string;
  keywords: Record<string, unknown>;
  optimization_specification: OptimizationSpecification;
  singlepoint_specification: QCSpecification;
};

export type NEBRecord = BaseRecord & {
  record_type: "neb";
  specification: NEBSpecification;
  initial_chain_molecule_ids: number[];
  initial_chain?: Molecule[];
  singlepoints?: Record<string, unknown>[];
  optimizations?: Record<string, unknown>;
};
