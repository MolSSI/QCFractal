import React from "react";
import {
  GridoptimizationDatasetEntry,
  GridoptimizationRecord,
  GridoptimizationSpecification,
} from "../../portal_types/gridoptimization";
import {
  EntryComponentProps,
  RecordComponentProps,
  MoleculeQueryOption,
  SpecificationComponentProps,
} from "./types";
import { RenderSpecification } from "./RenderSpecification.tsx";
import { RenderEntry } from "./RenderEntry.tsx";

// The server's query model requires optimization_program; null means no filter
export const moleculeQueryOptions: MoleculeQueryOption[] = [
  { field: "initial_molecule_id", extraFilters: { optimization_program: null } },
];

export const Specification: React.FC<
  SpecificationComponentProps<GridoptimizationSpecification>
> = ({ specification }) => {
  return (
    <RenderSpecification
      data={specification}
      keys={["program", "keywords", "optimization_specification"]}
    />
  );
};

export const DatasetEntry: React.FC<
  EntryComponentProps<GridoptimizationDatasetEntry>
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
      molecules={[entry.initial_molecule]}
    />
  );
};

export const RecordDetails: React.FC<
  RecordComponentProps<GridoptimizationRecord>
> = () => {
  return <></>;
};
