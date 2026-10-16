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

export type RecordModule = {
  RecordDetails: React.FC<RecordComponentProps<any>>;
  DatasetEntry: React.FC<EntryComponentProps<any>>;
  Specification: React.FC<SpecificationComponentProps<any>>;
}