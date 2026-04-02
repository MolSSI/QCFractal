import React from "react";
import {
  TorsiondriveDatasetEntry,
  TorsiondriveRecord,
  TorsiondriveSpecification,
} from "../../portal_types/torsiondrive";
import {
  EntryComponentProps,
  RecordComponentProps,
  SpecificationComponentProps,
} from "./types";
import { Specification as OptimizationSpecification } from "./optimization";
import { RenderSpecification } from "./RenderSpecification.tsx";
import { RenderEntry } from "./RenderEntry.tsx";

export const Specification: React.FC<
  SpecificationComponentProps<TorsiondriveSpecification>
> = ({ specification }) => {
  return (
    <RenderSpecification
      data={specification}
      keys={[
        "program",
        "keywords",
        {
          key: "optimization_specification",
          label: "Optimization Specification",
          render: () => (
            <OptimizationSpecification
              specification={specification.optimization_specification}
            />
          ),
        },
      ]}
    />
  );
};

export const DatasetEntry: React.FC<
  EntryComponentProps<TorsiondriveDatasetEntry>
> = ({ entry }) => {
  return (
    <RenderEntry
      data={entry}
      keys={[
        "name",
        "additional_keywords",
        "additional_optimization_keywords",
        "attributes",
        "comment",
      ]}
      molecules={entry.initial_molecules}
    />
  );
};

export const RecordDetails: React.FC<
  RecordComponentProps<TorsiondriveRecord>
> = () => {
  return <></>;
};