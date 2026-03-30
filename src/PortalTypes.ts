export * from "./portal_types/common";
export * from "./portal_types/singlepoint";
export * from "./portal_types/optimization";
export * from "./portal_types/torsiondrive";
export * from "./portal_types/gridoptimization";
export * from "./portal_types/reaction";
export * from "./portal_types/manybody";
export * from "./portal_types/neb";

import { SinglepointRecord } from "./portal_types/singlepoint";
import { OptimizationRecord } from "./portal_types/optimization";
import { TorsiondriveRecord } from "./portal_types/torsiondrive";
import { GridoptimizationRecord } from "./portal_types/gridoptimization";
import { ReactionRecord } from "./portal_types/reaction";
import { ManybodyRecord } from "./portal_types/manybody";
import { NEBRecord } from "./portal_types/neb";

export type RecordData =
  | SinglepointRecord
  | OptimizationRecord
  | TorsiondriveRecord
  | GridoptimizationRecord
  | ReactionRecord
  | ManybodyRecord
  | NEBRecord;
