import React from "react";
import {
  TorsiondriveDatasetEntry,
  TorsiondriveOptimization,
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
import { usePortalClient } from "../../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
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
import Properties from "../Properties.tsx";
import LoadingIndicator from "../LoadingIndicator.tsx";
import ErrorIndicator from "../ErrorIndicator.tsx";
import { MultiMoleculeViewer } from "../Molecule.tsx";

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
> = ({ recordData }) => {
  const { makeRequest } = usePortalClient();

  const includeFields: string[] = [];
  if (!recordData.initial_molecules) {
    includeFields.push("initial_molecules");
  }
  if (!recordData.optimizations) {
    includeFields.push("optimizations");
  }

  const { status: recordQueryStatus, data: extraRecordData } = useQuery({
    queryKey: ["torsiondriveRecordExtraData", recordData.id, includeFields],
    queryFn: () =>
      makeRequest<TorsiondriveRecord>(
        "GET",
        `api/v1/records/torsiondrive/${recordData.id}`,
        undefined,
        { include: includeFields },
      ),
    enabled: includeFields.length > 0,
  });

  const initialMolecules =
    recordData.initial_molecules || extraRecordData?.initial_molecules || [];
  const optimizations =
    recordData.optimizations || extraRecordData?.optimizations || [];

  const moleculeTuples = initialMolecules.map((molecule, index) => [
    `${molecule.identifiers?.molecular_formula || "Molecule"} ${index + 1} (ID ${molecule.id})`,
    molecule,
  ] as const);

  const groupedOptimizations = optimizations
    .slice()
    .sort(
      (a, b) =>
        a.key.localeCompare(b.key) ||
        a.position - b.position ||
        a.optimization_id - b.optimization_id,
    )
    .reduce<
      Array<{ key: string; optimizations: TorsiondriveOptimization[] }>
    >((groups, optimization) => {
      const lastGroup = groups[groups.length - 1];
      if (lastGroup && lastGroup.key === optimization.key) {
        lastGroup.optimizations.push(optimization);
      } else {
        groups.push({ key: optimization.key, optimizations: [optimization] });
      }
      return groups;
    }, []);

  return (
    <>
      <Grid
        container
        spacing={2}
        sx={{ mt: 2, width: "100%" }}
        alignItems="flex-start"
      >
        <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
          <Box sx={{ p: 2, height: "100%" }}>
            <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
              Specification
            </Typography>
            <Specification specification={recordData.specification} />
          </Box>
        </Grid>

        <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
          <Properties properties={recordData.properties} />
        </Grid>

        <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
          <Box
            sx={{
              p: 1,
              display: "flex",
              flexDirection: "column",
            }}
          >
            <Typography
              variant="h6"
              fontWeight="bold"
              sx={{ mb: 1, textAlign: "center" }}
            >
              Initial Molecules
            </Typography>
            <Box
              sx={{
                width: "100%",
                display: "flex",
                alignItems: "stretch",
                justifyContent: "center",
              }}
            >
              {recordQueryStatus === "pending" && !recordData.initial_molecules ? (
                <LoadingIndicator message="Loading initial molecules..." />
              ) : recordQueryStatus === "error" && !recordData.initial_molecules ? (
                <ErrorIndicator message="Failed to load initial molecules." />
              ) : moleculeTuples.length > 0 ? (
                <MultiMoleculeViewer
                  molecules={moleculeTuples.map(([label, molecule]) => [
                    label,
                    molecule,
                  ])}
                  height={280}
                />
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
                  No initial molecules available.
                </Typography>
              )}
            </Box>
          </Box>
        </Grid>
      </Grid>

      <Grid container spacing={2} sx={{ mt: 2, width: "100%" }}>
        <Grid size={{ xs: 12 }}>
          <Box
            sx={{
              p: 2,
              bgcolor: "background.paper",
              borderRadius: 1,
            }}
          >
            <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
              Optimizations ({optimizations.length})
            </Typography>

            {recordQueryStatus === "pending" && !recordData.optimizations ? (
              <LoadingIndicator message="Loading optimizations..." />
            ) : recordQueryStatus === "error" && !recordData.optimizations ? (
              <ErrorIndicator message="Failed to load optimizations." />
            ) : groupedOptimizations.length > 0 ? (
              <TableContainer component={Paper}>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>
                        <strong>Angles</strong>
                      </TableCell>
                      <TableCell>
                        <strong>Record ID</strong>
                      </TableCell>
                      <TableCell align="right">
                        <strong>Energy (Eh)</strong>
                      </TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {groupedOptimizations.flatMap((group) =>
                      group.optimizations.map((optimization, index) => (
                        <TableRow
                          key={`${group.key}-${optimization.position}-${optimization.optimization_id}`}
                        >
                          {index === 0 && (
                            <TableCell rowSpan={group.optimizations.length}>
                              {group.key}
                            </TableCell>
                          )}
                          <TableCell>
                            <Link
                              to={`/records/${optimization.optimization_id}`}
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              {optimization.optimization_id}
                            </Link>
                          </TableCell>
                          <TableCell align="right">
                            {optimization.energy?.toFixed(8) ?? "N/A"}
                          </TableCell>
                        </TableRow>
                      )),
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            ) : (
              <Typography color="text.secondary">
                No optimizations available.
              </Typography>
            )}
          </Box>
        </Grid>
      </Grid>
    </>
  );
};
