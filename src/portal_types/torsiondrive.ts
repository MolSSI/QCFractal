import { BaseRecord, Molecule } from "./common";
import { OptimizationSpecification, OptimizationRecord } from "./optimization";

export type TorsiondriveSpecification = {
  program: string;
  keywords: Record<string, unknown>;
  optimization_specification: OptimizationSpecification;
};

export type TorsiondriveRecord = BaseRecord & {
  record_type: "torsiondrive";
  specification: TorsiondriveSpecification;
  initial_molecules_ids: number[];
  initial_molecules?: Molecule[];
  optimizations?: Record<string, OptimizationRecord[]>;
};

export type TorsiondriveDatasetSpecification = {
  name: string;
  specification: TorsiondriveSpecification;
  description?: string;
};

export type TorsiondriveDatasetEntry = {
  name: string;
  initial_molecules: Molecule[];
  additional_keywords: Record<string, unknown>;
  additional_optimization_keywords: Record<string, unknown>;
  attributes: Record<string, unknown>;
  comment?: string;
};
