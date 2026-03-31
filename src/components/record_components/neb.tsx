import React from "react";
import {
  NEBDatasetEntry,
  NEBRecord,
  NEBSpecification,
} from "../../portal_types/neb";
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
  SpecificationComponentProps<NEBSpecification>
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
          showIfEmpty: false,
          render: () =>
            specification.optimization_specification ? (
              <OptimizationSpecification
                specification={specification.optimization_specification}
              />
            ) : null,
        },
        {
          key: "singlepoint_specification",
          label: "Singlepoint Specification",
          render: () => (
            <SinglepointSpecification
              specification={specification.singlepoint_specification}
            />
          ),
        },
      ]}
    />
  );
};

export const DatasetEntry: React.FC<EntryComponentProps<NEBDatasetEntry>> = ({
  entry,
}) => {
  return (
    <RenderEntry
      data={entry}
      keys={[
        "name",
        "additional_keywords",
        "additional_singlepoint_keywords",
        "attributes",
        "comment",
      ]}
      molecules={entry.initial_chain}
    />
  );
};

export const RecordDetails: React.FC<RecordComponentProps<NEBRecord>> = () => {
  return <></>;
};
