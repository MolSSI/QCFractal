import { usePortalClient } from "../PortalClient.tsx";
import React from "react";
import * as qcpTypes from "../PortalTypes";
import { useParams } from "react-router-dom";
import {
  Box,
  Chip,
  Grid,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import { parseToDate } from "../Utils";
import { useQuery } from "@tanstack/react-query";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";

export default function Manager() {
  const { managerName } = useParams();

  const { makeRequest } = usePortalClient();

  const {
    status,
    data: managerData,
    error,
  } = useQuery({
    queryKey: ["managerInfo", managerName],
    queryFn: () =>
      makeRequest<qcpTypes.Manager>("GET", `api/v1/managers/${managerName}`),
    enabled: !!managerName,
  });

  if (!managerName) {
    return (
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
        <ErrorIndicator message="Missing manager name" />
      </Box>
    );
  }

  const mCreatedOn = parseToDate(managerData?.created_on);
  const mLastUpdated = parseToDate(managerData?.modified_on);

  return (
    <>
      {status === "pending" && (
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
          <LoadingIndicator />
        </Box>
      )}

      {status === "error" && (
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
          <ErrorIndicator message={error.message} />
        </Box>
      )}

      {status === "success" && managerData && (
        <Grid container spacing={2} width="100%">
          <Grid size={12}>
            <Stack>
              <Typography variant="h4" fontWeight="bold">
                {managerData.name}
              </Typography>
              <Chip
                label={managerData.status}
                color={managerData.status == "active" ? "success" : "default"}
                sx={{ width: "fit-content", fontWeight: "bold" }}
              />
            </Stack>
          </Grid>
          <Grid size={6}>
            <Typography variant="body1">
              Manager version: {managerData.manager_version}
            </Typography>
            <Typography variant="body1">
              Created: {mCreatedOn?.toLocaleString()}
            </Typography>
            <Typography variant="body1">
              Last seen: {mLastUpdated?.toLocaleString()}
            </Typography>
            <Typography variant="body1">
              Cluster: {managerData.cluster}
            </Typography>
            <Typography variant="body1">
              Hostname: {managerData.hostname}
            </Typography>
          </Grid>
          <Grid size={6}>
            <Typography variant="h6">Tags</Typography>
            <ul>
              {managerData.tags.map((tag, index) => (
                <li key={index}>{tag}</li>
              ))}
            </ul>
          </Grid>
          <Grid size={4}>
            <TableContainer component={Paper}>
              <Table size="small" aria-label="manager programs table">
                <TableHead>
                  <TableRow>
                    <TableCell>
                      <strong>Program</strong>
                    </TableCell>
                    <TableCell>
                      <strong>Versions</strong>
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {Object.entries(managerData.programs).map(([p, v]) => {
                    return (
                      <React.Fragment key={p}>
                        <TableRow>
                          <TableCell>{p}</TableCell>
                          <TableCell>{v ? v : "?"}</TableCell>
                        </TableRow>
                      </React.Fragment>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          </Grid>
        </Grid>
      )}
    </>
  );
}
