import React from "react";
import {
  OptimizationDatasetEntry,
  OptimizationRecord,
  OptimizationSpecification
} from "../../portal_types/optimization";
import { EntryComponentProps, RecordComponentProps, SpecificationComponentProps } from "./types";
import { Specification as SinglepointSpecification } from "./singlepoint";
import { RenderSpecification } from "./RenderSpecification.tsx";
import { RenderEntry } from "./RenderEntry.tsx";
import { usePortalClient } from "../../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes.ts";
import { Box, Grid, Typography } from "@mui/material";
import Properties from "../Properties.tsx";
import LoadingIndicator from "../LoadingIndicator.tsx";
import ErrorIndicator from "../ErrorIndicator.tsx";
import { MultiMoleculeViewer } from "../Molecule.tsx";

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
> = ({ recordData }) => {
  const { makeRequest } = usePortalClient();

  const includeFields: string[] = [];
  if (recordData.final_molecule_id && !recordData.final_molecule) {
    includeFields.push("final_molecule");
  }
  if (recordData.initial_molecule_id && !recordData.initial_molecule) {
    includeFields.push("initial_molecule");
  }

  const { status: moleculeStatus, data: moleculeData } = useQuery({
    queryKey: ["recordMolecules", recordData.id],
    queryFn: () =>
      makeRequest<Record<string, any>>(
        "GET",
        `api/v1/records/optimization/${recordData.id}`,
        undefined,
        { include: includeFields },
      ),
    enabled: includeFields.length > 0,
  });

  const initialMolecule = recordData.initial_molecule || moleculeData?.initial_molecule;
  const finalMolecule = recordData.final_molecule || moleculeData?.final_molecule;

  const displayMolecules: [string, qcpTypes.Molecule][] = [];
  if (initialMolecule) {
    displayMolecules.push(["initial", initialMolecule as qcpTypes.Molecule]);
  }
  if (finalMolecule) {
    displayMolecules.push(["final", finalMolecule as qcpTypes.Molecule]);
  }

  return (
    <>
      {/* Specification, Properties, and Molecule Viewer */}
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

        {/* Molecule Viewer */}
        <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
          <Box
            sx={{
              p: 1,
              display: "flex",
              flexDirection: "column",
            }}
          >
            <Box
              sx={{
                width: "100%",
                display: "flex",
                alignItems: "stretch",
                justifyContent: "center",
                borderRadius: 1,
                overflow: "hidden",
                bgcolor: "background.paper",
              }}
            >
              {moleculeStatus === "pending" ? (
                <LoadingIndicator message="Loading molecule..." />
              ) : moleculeStatus === "error" ? (
                <ErrorIndicator message="Failed to load molecule." />
              ) : (
                <MultiMoleculeViewer
                  molecules={displayMolecules}
                />
              )}
            </Box>
          </Box>
        </Grid>
      </Grid>
    </>
  );
};
