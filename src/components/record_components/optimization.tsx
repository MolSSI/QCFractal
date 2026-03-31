import React from "react";
import {
  OptimizationDatasetEntry,
  OptimizationRecord,
  OptimizationSpecification,
} from "../../portal_types/optimization";
import {
  EntryComponentProps,
  RecordComponentProps,
  SpecificationComponentProps,
} from "./types";
import { Specification as SinglepointSpecification } from "./singlepoint";
import { RenderSpecification } from "./RenderSpecification.tsx";
import { RenderEntry } from "./RenderEntry.tsx";

export const Specification: React.FC<
  SpecificationComponentProps<OptimizationSpecification>
> = ({ specification }) => {
  return (
    <RenderSpecification
      data={specification}
      keys={[
        "program",
        "protocols",
        "keywords",
        {
          key: "qc_specification",
          label: "QC Specification",
          render: () => (
            <SinglepointSpecification
              specification={specification.qc_specification}
            />
          ),
        },
      ]}
    />
  );
};

export const DatasetEntry: React.FC<
  EntryComponentProps<OptimizationDatasetEntry>
> = ({ entry }) => {
  return (
    <RenderEntry
      data={entry}
      keys={["name", "additional_keywords", "attributes", "comment"]}
      molecules={[entry.initial_molecule]}
    />
  );
};

export const RecordDetails: React.FC<
  RecordComponentProps<OptimizationRecord>
> = () => {
  return <></>;
};
