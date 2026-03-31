import React from "react";
import {
  ReactionDatasetEntry,
  ReactionRecord,
  ReactionSpecification,
} from "../../portal_types/reaction";
import {
  EntryComponentProps,
  RecordComponentProps,
  SpecificationComponentProps,
} from "./types";
import { Specification as SinglepointSpecification } from "./singlepoint";
import { Specification as OptimizationSpecification } from "./optimization";
import { RenderSpecification } from "./RenderSpecification.tsx";
import { RenderEntry } from "./RenderEntry.tsx";

export const Specification: React.FC<
  SpecificationComponentProps<ReactionSpecification>
> = ({ specification }) => {
  return (
    <RenderSpecification
      data={specification}
      keys={[
        "program",
        "keywords",
        {
          key: "singlepoint_specification",
          label: "Singlepoint Specification",
          showIfEmpty: false,
          render: () =>
            specification.singlepoint_specification ? (
              <SinglepointSpecification
                specification={specification.singlepoint_specification}
              />
            ) : null,
        },
        {
          key: "optimization_specification",
          label: "Optimization Specification",
          showIfEmpty: false,
          render: () =>
            specification.optimization_specification ? (
              <OptimizationSpecification
                specification={specification.optimization_specification}
              />
            ) : null,
        },
      ]}
    />
  );
};

export const DatasetEntry: React.FC<
  EntryComponentProps<ReactionDatasetEntry>
> = ({ entry }) => {
  const molecules = entry.stoichiometries.map((s) => s.molecule);
  return (
    <RenderEntry
      data={entry}
      keys={["name", "additional_keywords", "attributes", "comment"]}
      molecules={molecules}
    />
  );
};

export const RecordDetails: React.FC<
  RecordComponentProps<ReactionRecord>
> = () => {
  return <></>;
};
