import {
  FetchedData,
  usePortalClient,
} from "../PortalClient.tsx";
import React, { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { Chip, Grid, Stack, Typography } from "@mui/material";
import { PieChart } from "@mui/x-charts/PieChart";
import { parseToDate } from "../Utils";

export const ManagerFragment: React.FC<{ managerName: string }> = ({
  managerName,
}) => {
  const { fetchData } = usePortalClient(); // Get client instance here

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

  const returned = managerData
    ? managerData.claimed -
      (managerData.active_tasks + managerData.successes + managerData.failures)
    : 0;

  return (
    <>
      {managerFetchedData.loading && <Typography>Loading...</Typography>}

      {!managerFetchedData.loading && managerData && (
        <Grid container spacing={2} width="100%">
          <Grid size={12}>
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
            <Stack>
              <Typography variant="h6">
                Tasks ({managerData.claimed} total claimed)
              </Typography>
              {managerData.claimed == 0 ? (
                <Typography variant="body1">(no claimed tasks)</Typography>
              ) : (
                <PieChart
                  colors={["blue", "green", "red", "orange"]}
                  series={[
                    {
                      data: [
                        {
                          id: 0,
                          value: managerData.active_tasks,
                          label: `${managerData.active_tasks} active`,
                        },
                        {
                          id: 1,
                          value: managerData.successes,
                          label: `${managerData.successes} success`,
                        },
                        {
                          id: 2,
                          value: managerData.failures,
                          label: `${managerData.failures} failed`,
                        },
                        {
                          id: 4,
                          value: returned,
                          label: `${returned} returned`,
                        },
                      ],
                    },
                  ]}
                />
              )}
            </Stack>
          </Grid>
        </Grid>
      )}
    </>
  );
};
