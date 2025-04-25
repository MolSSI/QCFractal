// src/pages/Profile.tsx
import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import React, { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { Chip, Grid, Stack, TableCell, TableRow, Typography } from "@mui/material";
import { parseToDate } from "../Utils";

export const ManagerFragment: React.FC<{ managerName: string }> = ({
  managerName,
}) => {
  const { fetchData } = usePortalClientRequest(); // Get client instance here

  const [managerFetchedData, setmanagerFetchedData] = useState<
    FetchedData<qcpTypes.Manager>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  useEffect(() => {
    fetchData<qcpTypes.Manager>(
      setmanagerFetchedData,
      "get",
      `api/v1/managers/${managerName}`,
    );
  }, [fetchData, managerName]);

  const managerData = managerFetchedData?.data;
  const mCreatedOn = parseToDate(managerData?.created_on);
  const mLastUpdated = parseToDate(managerData?.modified_on);

  return (
    <>
      {managerFetchedData.loading && <Typography>Loading...</Typography>}

      {!managerFetchedData.loading && managerData && (
        <Grid container spacing={2} width="100%">
          <Grid item size={12}>
            <Stack>
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
          <Grid item size={6}>
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
          <Grid item size={6}>
          <Typography variant="body1">
            Created: {mCreatedOn?.toLocaleString()}
          </Typography>
          <Typography variant="body1">
            Last seen: {mLastUpdated?.toLocaleString()}
          </Typography>
          </Grid>
          <Grid item size={6}>
            <Typography variant="h6">Programs</Typography>
            <ul>
              {Object.entries(managerData.programs).map(([p, v]) => {
                return (
                  <li key={p}>{p}</li>
                );
              })}
            </ul>
          </Grid>
          <Grid item size={6}>
            <Typography variant="h6">Tags</Typography>
            <ul>
              {managerData.tags.map((tag, index) => (
                <li key={index}>{tag}</li>
              ))}
            </ul>
          </Grid>
        </Grid>
      )}
    </>
  );
};
