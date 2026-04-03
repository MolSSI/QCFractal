import React from "react";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import { useParams } from "react-router-dom";
import {
  Box,
  Chip,
  Grid,
  Paper,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { useQuery } from "@tanstack/react-query";
import DatasetStatusTable from "../components/dataset_components/DatasetStatusTable";
import DatasetActions from "../components/dataset_components/DatasetActions";
import DatasetSpecificationTable from "../components/dataset_components/DatasetSpecificationTable";
import DatasetEntryTable from "../components/dataset_components/DatasetEntryTable";

function TabPanel(props: {
  children?: React.ReactNode;
  index: number;
  value: number;
}) {
  const { children, value, index, ...other } = props;
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`tabpanel-${index}`}
      aria-labelledby={`tab-${index}`}
      {...other}
    >
      {value === index && <Box p={2}>{children}</Box>}
    </div>
  );
}

export default function Dataset() {
  const { datasetId } = useParams();
  const { makeRequest } = usePortalClient();

  const [tabValue, setTabValue] = React.useState(0);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const {
    status: datasetStatus,
    data: datasetData,
    error: datasetError,
  } = useQuery({
    queryKey: ["dataset", datasetId],
    queryFn: () =>
      makeRequest<qcpTypes.Dataset>("GET", `api/v1/datasets/${datasetId}`),
    enabled: !!datasetId,
  });

  const { data: statusData } = useQuery({
    queryKey: ["datasetStatus", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetStatus>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/status`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const { data: specificationsData } = useQuery({
    queryKey: ["datasetSpecifications", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<Record<string, any>>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/specifications`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const { data: entryNamesData } = useQuery({
    queryKey: ["datasetEntryNames", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<string[]>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/entry_names`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const { data: recordCountData } = useQuery({
    queryKey: ["datasetRecordCount", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<number>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/record_count`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  if (!datasetId) {
    return <ErrorIndicator fullPage message="Missing dataset ID" />;
  }

  return (
    <>
      {datasetStatus === "pending" && <LoadingIndicator fullPage />}

      {datasetStatus === "error" && (
        <ErrorIndicator fullPage message={datasetError.message} />
      )}

      {datasetStatus === "success" && datasetData && (
        <Grid container spacing={2} width="100%">
          <Grid size={12}>
            <Box sx={{ display: "flex", alignItems: "center" }}>
              <Box>
                <Typography variant="h4" fontWeight="bold">
                  <Typography
                    variant="h5"
                    fontWeight="bold"
                    component={"span"}
                    pr={2}
                  >
                    [{datasetData.id}]
                  </Typography>
                  <Chip
                    label={datasetData.dataset_type}
                    color="primary"
                    variant="outlined"
                    sx={{ mr: 2, verticalAlign: "middle" }}
                  />
                  {datasetData.name}
                </Typography>
                <Typography
                  variant="subtitle1"
                  sx={{ color: "text.secondary" }}
                >
                  {datasetData.tagline}
                </Typography>
              </Box>
            </Box>
          </Grid>
          {/* Summary stats: for example, Specifications, Entries */}
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {entryNamesData ? entryNamesData.length : 0} Entries
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {specificationsData
                    ? Object.keys(specificationsData).length
                    : 0}{" "}
                  Specifications
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {recordCountData ? recordCountData : 0} Records
                </Typography>
              </Box>
            </Paper>
          </Grid>

          {/* Description & Metadata section */}
          <Grid size={12}>
            <Paper elevation={2}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold" gutterBottom>
                  Description
                </Typography>
                <Typography variant="body1" paragraph>
                  {datasetData.description}
                </Typography>

                <Typography variant="h6" fontWeight="bold" gutterBottom>
                  Tags
                </Typography>
                <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap", mb: 2 }}>
                  {datasetData.tags?.map((tag, idx) => (
                    <Chip key={idx} label={tag} variant="outlined" />
                  ))}
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Group
                  </Typography>
                  <Typography variant="body1">
                    {datasetData.group || "N/A"}
                  </Typography>
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Default Compute Tag
                  </Typography>
                  <Typography variant="body1">
                    {datasetData.default_compute_tag || "N/A"}
                  </Typography>
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Default Compute Priority
                  </Typography>
                  <Typography variant="body1">
                    {datasetData.default_compute_priority}
                  </Typography>
                </Box>
              </Box>
            </Paper>
          </Grid>

          {/* Status section */}
          <Grid size={6}>
            <Paper elevation={2}>
              <Box p={2} borderBottom={1} borderColor="divider">
                <Typography variant="h6" fontWeight="bold">
                  Status
                </Typography>
              </Box>
              <Box p={2}>
                {!statusData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetStatusTable statusData={statusData} />
                )}
              </Box>
            </Paper>
          </Grid>

          <Grid size={6}>
            <Paper elevation={2}>
              <Box p={2} borderBottom={1} borderColor="divider">
                <Typography variant="h6" fontWeight="bold">
                  Actions
                </Typography>
              </Box>
              <DatasetActions />
            </Paper>
          </Grid>

          {/* Tabs for Specifications, Entries */}
          <Grid size={12} mt={2}>
            <Paper elevation={2}>
              <Tabs
                value={tabValue}
                onChange={handleTabChange}
                aria-label="dataset sections tabs"
                variant="fullWidth"
              >
                <Tab
                  label="Specifications"
                  id="tab-0"
                  aria-controls="tabpanel-0"
                />
                <Tab label="Entries" id="tab-1" aria-controls="tabpanel-1" />
              </Tabs>

              {/* Tab 0: Specifications */}
              <TabPanel value={tabValue} index={0}>
                {!specificationsData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetSpecificationTable
                    specificationsData={specificationsData}
                    datasetType={datasetData.dataset_type}
                  />
                )}
              </TabPanel>

              {/* Tab 1: Entries */}
              <TabPanel value={tabValue} index={1}>
                {!entryNamesData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetEntryTable
                    entryNames={entryNamesData}
                    datasetType={datasetData.dataset_type}
                    datasetId={datasetId}
                  />
                )}
              </TabPanel>
            </Paper>
          </Grid>
        </Grid>
      )}
    </>
  );
}
