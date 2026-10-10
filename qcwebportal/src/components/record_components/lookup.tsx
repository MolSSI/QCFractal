import * as singlepoint from "./singlepoint";
import * as optimization from "./optimization";
import * as gridoptimization from "./gridoptimization";
import * as torsiondrive from "./torsiondrive";
import * as reaction from "./reaction";
import * as manybody from "./manybody";
import * as neb from "./neb";

import * as qcpTypes from "../../PortalTypes";
import {
  EntryComponentProps,
  RecordComponentProps,
  RecordModule,
  SpecificationComponentProps,
} from "./types.ts";
import React from "react";

const recordModules = {
  singlepoint,
  optimization,
  gridoptimization,
  torsiondrive,
  reaction,
  manybody,
  neb,
} satisfies Record<qcpTypes.RecordType, RecordModule>;

export const getRecordDetailsComponent = (
  recordType: qcpTypes.RecordType,
): React.FC<RecordComponentProps<any>> => {
  return recordModules[recordType].RecordDetails;
};

export const getDatasetEntryComponent = (
  recordType: qcpTypes.RecordType,
): React.FC<EntryComponentProps<any>> => {
  return recordModules[recordType].DatasetEntry;
};

export const getSpecificationComponent = (
  recordType: qcpTypes.RecordType,
): React.FC<SpecificationComponentProps<any>> => {
  return recordModules[recordType].Specification;
};