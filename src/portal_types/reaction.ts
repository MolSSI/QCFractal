import { BaseRecord, QCSpecification } from "./common";
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
