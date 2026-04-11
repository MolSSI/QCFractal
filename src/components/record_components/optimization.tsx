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
import { usePortalClient } from "../../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes.ts";
import { Link } from "react-router-dom";
import {
  Box,
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import { LineChart } from "@mui/x-charts/LineChart";
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
  if (recordData.status === "complete" && !recordData.trajectory_ids) {
    includeFields.push("trajectory");
  }

  const { status: recordQueryStatus, data: extraRecordData } = useQuery({
    queryKey: ["recordExtraData", recordData.id],
    queryFn: () =>
      makeRequest<Record<string, any>>(
        "GET",
        `api/v1/records/optimization/${recordData.id}`,
        undefined,
        { include: includeFields },
      ),
    enabled: includeFields.length > 0,
  });

  const initialMolecule =
    recordData.initial_molecule || extraRecordData?.initial_molecule;
  const finalMolecule =
    recordData.final_molecule || extraRecordData?.final_molecule;
  const trajectory =
    recordData.trajectory_ids || extraRecordData?.trajectory_ids || [];
  const energies = recordData.energies || [];

  const relativeEnergies =
    energies.length > 0 ? energies.map((e) => (e - energies[0]) * 1000) : [];

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
              {recordQueryStatus === "pending" ? (
                <LoadingIndicator message="Loading molecule..." />
              ) : recordQueryStatus === "error" ? (
                <ErrorIndicator message="Failed to load molecule." />
              ) : (
                <MultiMoleculeViewer molecules={displayMolecules} />
              )}
            </Box>
          </Box>
        </Grid>
      </Grid>

      {/* Trajectory and Energies */}
      {energies.length > 0 && (
        <Grid container spacing={2} sx={{ mt: 2, width: "100%" }}>
          {/* Energy Graph */}
          <Grid size={{ xs: 12, lg: 8 }}>
            <Box sx={{ p: 2, bgcolor: "background.paper", borderRadius: 1 }}>
              <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
                Optimization Energies (millihartrees relative to first step)
              </Typography>
              <Box sx={{ width: "100%", height: 350 }}>
                <LineChart
                  xAxis={[
                    {
                      data: energies.map((_, i) => i),
                      tickLabelInterval: "auto",
                      label: "Step",
                      height: 60,
                    },
                  ]}
                  yAxis={[
                    { valueFormatter: (value: number) => value.toFixed(1) },
                  ]}
                  series={[
                    {
                      data: relativeEnergies,
                      label: "Relative Energy (mEh)",
                      showMark: true,
                    },
                  ]}
                  height={300}
                  margin={{ left: 20, right: 30, top: 15, bottom: 10 }}
                />
              </Box>
            </Box>
          </Grid>

          {/* Trajectory List */}
          <Grid size={{ xs: 12, lg: 4 }}>
            <Box
              sx={{
                p: 2,
                bgcolor: "background.paper",
                borderRadius: 1,
                height: "100%",
              }}
            >
              <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
                Trajectory ({trajectory.length})
              </Typography>
              <TableContainer component={Paper} sx={{ maxHeight: 350 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell>
                        <strong>Step</strong>
                      </TableCell>
                      <TableCell>
                        <strong>Record ID</strong>
                      </TableCell>
                      <TableCell align="right">
                        <strong>Energy (Eh)</strong>
                      </TableCell>
                      <TableCell align="right">
                        <strong>Relative (mEh)</strong>
                      </TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {trajectory.length > 0 ? (
                      trajectory.map((step: number, index: number) => (
                        <TableRow key={step}>
                          <TableCell>{index}</TableCell>
                          <TableCell>
                            <Link
                              to={`/records/${step}`}
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              {step}
                            </Link>
                          </TableCell>
                          <TableCell align="right">
                            {energies[index]?.toFixed(8) ?? "N/A"}
                          </TableCell>
                          <TableCell align="right">
                            {relativeEnergies[index]?.toFixed(6) ?? "N/A"}
                          </TableCell>
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={4} align="center">
                          {recordData.status === "complete"
                            ? "No trajectory available."
                            : "Trajectory available upon completion."}
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            </Box>
          </Grid>
        </Grid>
      )}
    </>
  );
};
