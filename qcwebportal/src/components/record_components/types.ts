import React from "react";

export interface SpecificationComponentProps<S> {
  specification: S;
}

export interface EntryComponentProps<DE> {
  entry: DE;
}

export interface DatasetSpecificationComponentProps<DS> {
  specification: DS;
}

export interface RecordComponentProps<R> {
  recordData: R;
}

// A record query filter field that references a molecule. The label
// distinguishes types with more than one such field (optimization initial/final).
export type MoleculeQueryOption = {
  label?: string;
  field: string;
};

export type RecordModule = {
  RecordDetails: React.FC<RecordComponentProps<any>>;
  DatasetEntry: React.FC<EntryComponentProps<any>>;
  Specification: React.FC<SpecificationComponentProps<any>>;
  moleculeQueryOptions: MoleculeQueryOption[];
}