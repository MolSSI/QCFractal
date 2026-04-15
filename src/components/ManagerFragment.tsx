import { usePortalClient } from "../PortalClient.tsx";
import React from "react";
import * as qcpTypes from "../PortalTypes";
import { Chip, Grid, Stack, Typography } from "@mui/material";
import { parseToDate } from "../Utils";
import { useQuery } from "@tanstack/react-query";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import { ManagerPieChart } from "./ManagerPieChart.tsx";

export const ManagerFragment: React.FC<{ managerName: string }> = ({
  managerName,
}) => {
  const { makeRequest } = usePortalClient();

  const {
    status,
    data: managerData,
    error,
  } = useQuery({
    queryKey: ["managerInfo", managerName],
    queryFn: () =>
      makeRequest<qcpTypes.Manager>("GET", `/api/v1/managers/${managerName}`),
  });

  if (status == "pending") {
    return <LoadingIndicator />;
  }

  if (status == "error") {
    return <ErrorIndicator message={error.message} />;
  }

  const mCreatedOn = parseToDate(managerData.created_on);
  const mLastUpdated = parseToDate(managerData.modified_on);

  return (
    <>
      <Grid container spacing={2} width="100%">
        <Grid size={12}>
          <Stack gap={2}>
            <Typography variant="h6" fontWeight="bold">
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
            <strong>Manager version:</strong> {managerData.manager_version}
          </Typography>
          <Typography variant="body1">
            <strong>Cluster:</strong>
            {managerData.cluster}
          </Typography>
          <Typography variant="body1">
            <strong>Hostname:</strong> {managerData.hostname}
          </Typography>
        </Grid>
        <Grid size={6}>
          <Typography variant="body1">
            <strong>Created:</strong> {mCreatedOn?.toLocaleString()}
          </Typography>
          <Typography variant="body1">
            <strong>Last seen:</strong> {mLastUpdated?.toLocaleString()}
          </Typography>
        </Grid>
        <Grid size={6}>
          <Typography variant="h6">Programs</Typography>
          <ul>
            {Object.entries(managerData.programs).map(([p]) => {
              return <li key={p}>{p}</li>;
            })}
          </ul>
        </Grid>
        <Grid size={6}>
          <Typography variant="h6">Tags</Typography>
          <ul>
            {managerData.tags.map((tag, index) => (
              <li key={index}>{tag}</li>
            ))}
          </ul>
        </Grid>
        <Grid size={6}>
          <ManagerPieChart managerData={managerData} />
        </Grid>
      </Grid>
    </>
  );
};
