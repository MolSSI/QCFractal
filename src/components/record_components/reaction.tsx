import React from "react";
import {
  ReactionComponentMeta,
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
import { Box, Grid, Typography } from "@mui/material";
import Properties from "../Properties.tsx";
import LoadingIndicator from "../LoadingIndicator.tsx";
import ErrorIndicator from "../ErrorIndicator.tsx";
import { MultiMoleculeViewer } from "../Molecule.tsx";
import { useQuery } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes.ts";
import { usePortalClient } from "../../PortalClient.tsx";

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
> = ({ recordData }) => {
  const { makeRequest } = usePortalClient();

  const { status: componentsStatus, data: componentsData } = useQuery({
    queryKey: ["reaction_components", recordData.id],
    queryFn: () =>
      makeRequest<ReactionComponentMeta[]>(
        "GET",
        `api/v1/records/reaction/${recordData.id}/components`,
      ),
    enabled: !recordData.components,
  });

  const displayComponents = recordData.components || componentsData;

  const moleculeTuples: [string, qcpTypes.Molecule][] =
    displayComponents?.map((comp) => {
      const name = `${comp.coefficient > 0 ? "+" : ""}${comp.coefficient} ${comp.molecule?.identifiers.molecular_formula || "Molecule"} (ID ${comp.molecule_id})`;
      return [name, comp.molecule as qcpTypes.Molecule];
    }) || [];

  return (
    <>
      <Grid
        container
        spacing={2}
        sx={{ mt: 2, width: "100%" }}
        alignItems="flex-start"
      >
        {/* Specification */}
        <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
          <Box sx={{ p: 2, height: "100%" }}>
            <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
              Specification
            </Typography>
            <Specification specification={recordData.specification} />
          </Box>
        </Grid>

        {/* Properties */}
        <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
          <Properties properties={recordData.properties} />
        </Grid>

        {/* Multi-Molecule Viewer */}
        <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
          <Box
            sx={{
              p: 1,
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
            }}
          >
            <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
              Reaction Components
            </Typography>
            <Box
              sx={{
                width: "100%",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
              }}
            >
              {componentsStatus === "pending" ? (
                <LoadingIndicator message="Loading components..." />
              ) : componentsStatus === "error" ? (
                <ErrorIndicator message="Failed to load components." />
              ) : moleculeTuples.length > 0 ? (
                <MultiMoleculeViewer molecules={moleculeTuples} height={280} />
              ) : (
                <Typography
                  sx={{
                    color: "warning.main",
                    fontSize: "1rem",
                    textAlign: "center",
                    px: 2,
                    fontWeight: 500,
                  }}
                >
                  No components available for this reaction.
                </Typography>
              )}
            </Box>
          </Box>
        </Grid>
      </Grid>
    </>
  );
};
