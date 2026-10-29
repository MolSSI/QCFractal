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

export type MoleculeRecordOption = {
  key: string;
  label: string;
  recordType: qcpTypes.RecordType;
  field: string;
  extraFilters: Record<string, unknown>;
};

export const getMoleculeRecordOptions = (): MoleculeRecordOption[] => {
  return (Object.keys(recordModules) as qcpTypes.RecordType[]).flatMap(
    (recordType) =>
      recordModules[recordType].moleculeQueryOptions.map((option) => ({
        key: option.label ? `${recordType}:${option.label}` : recordType,
        label: option.label ? `${recordType} (${option.label})` : recordType,
        recordType,
        field: option.field,
        extraFilters: option.extraFilters ?? {},
      })),
  );
};