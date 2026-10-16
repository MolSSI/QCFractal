import React from "react";
import { QCSpecification } from "../../portal_types/common";
import {
  SinglepointDatasetEntry,
  SinglepointRecord,
} from "../../portal_types/singlepoint";
import {
  EntryComponentProps,
  RecordComponentProps,
  SpecificationComponentProps,
} from "./types";
import { RenderSpecification } from "./RenderSpecification.tsx";
import { RenderEntry } from "./RenderEntry.tsx";
import { Box, Grid, Typography } from "@mui/material";
import Properties from "../Properties.tsx";
import LoadingIndicator from "../LoadingIndicator.tsx";
import ErrorIndicator from "../ErrorIndicator.tsx";
import { SingleMoleculeViewer } from "../Molecule.tsx";
import { useQuery } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes.ts";
import { usePortalClient } from "../../PortalClient.tsx";

export const Specification: React.FC<
  SpecificationComponentProps<QCSpecification>
> = ({ specification }) => {
  return (
    <RenderSpecification
      data={specification}
      keys={["program", "driver", "method", "basis", "protocols", "keywords"]}
    />
  );
};

export const DatasetEntry: React.FC<
  EntryComponentProps<SinglepointDatasetEntry>
> = ({ entry }) => {
  return (
    <RenderEntry
      data={entry}
      keys={[
        "name",
        "additional_keywords",
        "attributes",
        "comment",
        "local_results",
      ]}
      molecules={[entry.molecule]}
    />
  );
};

export const RecordDetails: React.FC<
  RecordComponentProps<SinglepointRecord>
> = ({ recordData }) => {
  const { makeRequest } = usePortalClient();

  const { status: moleculeStatus, data: moleculeData } = useQuery({
    queryKey: ["molecule", recordData.molecule_id],
    queryFn: () =>
      makeRequest<qcpTypes.Molecule>(
        "GET",
        `api/v1/molecules/${recordData.molecule_id}`,
      ),
    enabled: !recordData.molecule,
  });

  const displayMolecule = recordData.molecule || moleculeData;

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
          {moleculeStatus === "pending" ? (
            <LoadingIndicator message="Loading molecule..." />
          ) : moleculeStatus === "error" ? (
            <ErrorIndicator message="Failed to load molecule." />
          ) : (
            <SingleMoleculeViewer
              molecule={displayMolecule}
              width="100%"
              height={300}
            />
          )}
        </Grid>
      </Grid>
    </>
  );
};