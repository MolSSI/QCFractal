import { BaseRecord, QCSpecification, Molecule } from "./common";
import { OptimizationSpecification } from "./optimization";

export type NEBSpecification = {
  program: string;
  keywords: Record<string, unknown>;
  optimization_specification?: OptimizationSpecification;
  singlepoint_specification: QCSpecification;
};


export type NEBSinglepoint = {
    singlepoint_id: number;
    chain_iteration: number;
    position: number;
};

export type NEBOptimization = {
    optimization_id: number;
    position: number;
    ts: boolean;
};

export type NEBRecord = BaseRecord & {
  record_type: "neb";
  specification: NEBSpecification;
  initial_chain_molecule_ids: number[];
  initial_chain?: Molecule[];
  singlepoints?: NEBSinglepoint[];
  optimizations?: Record<string, NEBOptimization>;
};

export type NEBDatasetSpecification = {
  name: string;
  specification: NEBSpecification;
  description?: string;
};

export type NEBDatasetEntry = {
  name: string;
  initial_chain: Molecule[];
  additional_keywords: Record<string, unknown>;
  additional_singlepoint_keywords: Record<string, unknown>;
  attributes: Record<string, unknown>;
  comment?: string;
};
