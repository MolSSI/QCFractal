import { usePortalClient } from "../PortalClient.tsx";
import React from "react";
import * as qcpTypes from "../PortalTypes";
import { Chip, Grid, Stack, Typography } from "@mui/material";
import { parseToDate } from "../Utils";
import { useQuery } from "@tanstack/react-query";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import { ManagerPieChart } from "./ManagerPieChart.tsx";
import { Link } from "react-router-dom";

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
            <Link
              to={`/managers/${managerData.name}`}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                color: "inherit",
                textDecoration: "underline",
              }}
            >
              <Typography variant="h6">Go to manager page</Typography>
            </Link>
          </Stack>
        </Grid>
        <Grid size={6}>
          <Typography variant="body1">
            Manager version: {managerData.manager_version}
          </Typography>
          <Typography variant="body1">
            Cluster: {managerData.cluster}
          </Typography>
          <Typography variant="body1">
            Hostname: {managerData.hostname}
          </Typography>
        </Grid>
        <Grid size={6}>
          <Typography variant="body1">
            Created: {mCreatedOn?.toLocaleString()}
          </Typography>
          <Typography variant="body1">
            Last seen: {mLastUpdated?.toLocaleString()}
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
