import { BaseRecord, Molecule } from "./common";
import { OptimizationSpecification, OptimizationRecord } from "./optimization";

export type GridoptimizationSpecification = {
  program: string;
  keywords: Record<string, unknown>;
  optimization_specification: OptimizationSpecification;
};

export type GridoptimizationRecord = BaseRecord & {
  record_type: "gridoptimization";
  specification: GridoptimizationSpecification;
  initial_molecule_id: number;
  initial_molecule?: Molecule;
  starting_molecule_id?: number;
  starting_molecule?: Molecule;
  optimizations?: Record<string, OptimizationRecord>;
};

export type GridoptimizationDatasetSpecification = {
  name: string;
  specification: GridoptimizationSpecification;
  description?: string;
};

export type GridoptimizationDatasetEntry = {
  name: string;
  initial_molecule: Molecule;
  additional_keywords: Record<string, unknown>;
  additional_optimization_keywords: Record<string, unknown>;
  attributes: Record<string, unknown>;
  comment?: string;
};
