import { usePortalClient } from "../PortalClient.tsx";
import React from "react";
import * as qcpTypes from "../PortalTypes";
import { Button, Chip, Grid, Stack, Typography } from "@mui/material";
import { dateStringToLocalTime } from "../Utils";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import { ManagerPieChart } from "./ManagerPieChart.tsx";

export const ManagerFragment: React.FC<{
  managerName: string;
  showViewButton?: boolean;
  onNavigate?: () => void;
}> = ({ managerName, showViewButton = false, onNavigate }) => {
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

  return (
    <>
      <Grid container spacing={2} width="100%">
        <Grid size={12}>
          <Stack gap={2}>
            <Typography variant="h6" fontWeight="bold">
              {managerData.name}
            </Typography>
            <Stack direction="row" gap={2} alignItems="center">
              <Chip
                label={managerData.status}
                color={managerData.status == "active" ? "success" : "default"}
                sx={{ width: "fit-content", fontWeight: "bold" }}
              />
              {showViewButton && (
                <Button
                  variant="contained"
                  size="small"
                  component={Link}
                  to={`/managers/${managerData.name}`}
                  onClick={onNavigate}
                >
                  Go to Manager Page
                </Button>
              )}
            </Stack>
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
            <strong>Created:</strong>{" "}
            {dateStringToLocalTime(managerData.created_on)}
          </Typography>
          <Typography variant="body1">
            <strong>Last seen:</strong>{" "}
            {dateStringToLocalTime(managerData.modified_on)}
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
