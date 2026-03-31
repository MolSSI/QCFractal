import React from "react";
import {
  ManybodyDatasetEntry,
  ManybodyRecord,
  ManybodySpecification,
} from "../../portal_types/manybody";
import {
  EntryComponentProps,
  RecordComponentProps,
  SpecificationComponentProps,
} from "./types";
import { Specification as SinglepointSpecification } from "./singlepoint";
import { RenderSpecification } from "./RenderSpecification.tsx";
import { RenderEntry } from "./RenderEntry.tsx";

export const Specification: React.FC<
  SpecificationComponentProps<ManybodySpecification>
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

export const DatasetEntry: React.FC<
  EntryComponentProps<ManybodyDatasetEntry>
> = ({ entry }) => {
  return (
    <RenderEntry
      data={entry}
      keys={[
        "name",
        "additional_singlepoint_keywords",
        "attributes",
        "comment",
      ]}
      molecules={[entry.initial_molecule]}
    />
  );
};

export const RecordDetails: React.FC<
  RecordComponentProps<ManybodyRecord>
> = () => {
  return <></>;
};
