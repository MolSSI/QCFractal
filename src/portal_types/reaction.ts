import { BaseRecord, QCSpecification, Molecule } from "./common";
import { OptimizationSpecification } from "./optimization";

export type ReactionSpecification = {
  program: string;
  keywords: Record<string, unknown>;
  singlepoint_specification?: QCSpecification;
  optimization_specification?: OptimizationSpecification;
};

export type ReactionRecord = BaseRecord & {
  record_type: "reaction";
  specification: ReactionSpecification;
  total_energy?: number;
  component_records?: Record<string, unknown>[];
};

export type ReactionDatasetSpecification = {
  name: string;
  specification: ReactionSpecification;
  description?: string;
};

export type ReactionDatasetEntryStoichiometry = {
  coefficient: number;
  molecule: Molecule;
};

export type ReactionDatasetEntry = {
  name: string;
  stoichiometries: ReactionDatasetEntryStoichiometry[];
  additional_keywords: Record<string, unknown>;
  attributes: Record<string, unknown>;
  comment?: string;
};
