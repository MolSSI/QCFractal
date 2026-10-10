export * from "./portal_types/common";
export * from "./portal_types/record_types.ts"
export * from "./portal_types/singlepoint";
export * from "./portal_types/optimization";
export * from "./portal_types/torsiondrive";
export * from "./portal_types/gridoptimization";
export * from "./portal_types/reaction";
export * from "./portal_types/manybody";
export * from "./portal_types/neb";

import { SinglepointRecord, SinglepointDatasetSpecification, SinglepointDatasetEntry } from "./portal_types/singlepoint";
import { OptimizationRecord, OptimizationDatasetSpecification, OptimizationDatasetEntry } from "./portal_types/optimization";
import { TorsiondriveRecord, TorsiondriveDatasetSpecification, TorsiondriveDatasetEntry } from "./portal_types/torsiondrive";
import { GridoptimizationRecord, GridoptimizationDatasetSpecification, GridoptimizationDatasetEntry } from "./portal_types/gridoptimization";
import { ReactionRecord, ReactionDatasetSpecification, ReactionDatasetEntry } from "./portal_types/reaction";
import { ManybodyRecord, ManybodyDatasetSpecification, ManybodyDatasetEntry } from "./portal_types/manybody";
import { NEBRecord, NEBDatasetSpecification, NEBDatasetEntry } from "./portal_types/neb";

export type RecordData =
  | SinglepointRecord
  | OptimizationRecord
  | TorsiondriveRecord
  | GridoptimizationRecord
  | ReactionRecord
  | ManybodyRecord
  | NEBRecord;

export type DatasetSpecificationData =
  | SinglepointDatasetSpecification
  | OptimizationDatasetSpecification
  | TorsiondriveDatasetSpecification
  | GridoptimizationDatasetSpecification
  | ReactionDatasetSpecification
  | ManybodyDatasetSpecification
  | NEBDatasetSpecification;

export type DatasetEntryData =
  | SinglepointDatasetEntry
  | OptimizationDatasetEntry
  | TorsiondriveDatasetEntry
  | GridoptimizationDatasetEntry
  | ReactionDatasetEntry
  | ManybodyDatasetEntry
  | NEBDatasetEntry;
