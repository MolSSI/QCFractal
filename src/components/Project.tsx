// src/pages/Profile.tsx
import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { useParams } from "react-router-dom";
import {
  Box,
  Button,
  Chip,
  Grid,
  Paper,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import DatasetTab from "./DatasetTab";
import RecordTab from "./RecordTab";

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

export default function Project() {
  const { projectId } = useParams();

  const { fetchData } = usePortalClientRequest(); // Get client instance here

  const [projectFetchedData, setProjectFetchedData] = useState<
    FetchedData<qcpTypes.Project>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  const [datasetMetadataFetchedData, setDatasetMetadataFetchedData] = useState<
    FetchedData<Array<qcpTypes.ProjectDatasetMetadata>>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  const [recordMetadataFetchedData, setRecordMetadataFetchedData] = useState<
    FetchedData<Array<qcpTypes.ProjectRecordMetadata>>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  // State for the Tabs
  const [tabValue, setTabValue] = useState(0);
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  // Load project data, then dataset & record metadata
  useEffect(() => {
    fetchData<qcpTypes.Project>(
      setProjectFetchedData,
      "get",
      `api/v1/projects/${projectId}`,
    );
  }, [fetchData, projectId]);

  // Fetch dataset metadata
  useEffect(() => {
    fetchData<Array<qcpTypes.ProjectDatasetMetadata>>(
      setDatasetMetadataFetchedData,
      "get",
      `api/v1/projects/${projectId}/dataset_metadata`,
    );
  }, [fetchData, projectId]);

  useEffect(() => {
    fetchData<Array<qcpTypes.ProjectRecordMetadata>>(
      setRecordMetadataFetchedData,
      "get",
      `api/v1/projects/${projectId}/record_metadata`,
    );
  }, [fetchData, projectId]);

  const projectData = projectFetchedData?.data;
  const datasetMetadata = datasetMetadataFetchedData?.data;
  const recordMetadata = recordMetadataFetchedData?.data;

  return (
    <>
      {projectFetchedData.loading && <Typography>Loading...</Typography>}

      {!projectFetchedData.loading && projectData && (
        <Grid container spacing={2} width="100%">
          {/* Project name & tagline */}
          <Grid size={12}>
            <Typography variant="h4" fontWeight="bold">
              {projectData.name}
            </Typography>
            <Typography variant="subtitle1" sx={{ color: "text.secondary" }}>
              {projectData.tagline}
            </Typography>
          </Grid>
          {/* Summary stats: for example, Records, Datasets */}
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {recordMetadata ? recordMetadata.length : 0} Records
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {datasetMetadata ? datasetMetadata.length : 0} Datasets
                </Typography>
              </Box>
            </Paper>
          </Grid>

          {/* Description & Metadata section */}
          <Box size={12} mx={"auto"}>
            <Grid size={12} mb={3}>
              <Paper elevation={2}>
                <Box
                  display="flex"
                  alignItems="center"
                  justifyContent="left"
                  p={2}
                >
                  <Typography variant="h6" fontWeight="bold">
                    Description & Metadata
                  </Typography>
                  <Button sx={{ ml: 2 }} variant="outlined" color="primary">
                    Edit
                  </Button>
                </Box>
                <Box p={2}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Description
                  </Typography>
                  <Typography variant="body1" paragraph>
                    {projectData.description}
                  </Typography>

                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Tags
                  </Typography>
                  <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
                    {projectData.tags?.map((tag, idx) => (
                      <Chip key={idx} label={tag} variant="outlined" />
                    ))}
                  </Box>

                  <Box mt={2}>
                    <Typography variant="h6" fontWeight="bold" gutterBottom>
                      Owner
                    </Typography>
                    <Typography variant="body1">
                      {projectData.owner_user || "N/A"}
                    </Typography>
                  </Box>
                </Box>
              </Paper>
            </Grid>

            {/* Lower section with Tabs for Datasets, Records */}
            <Grid size={12}>
              <Paper elevation={2}>
                <Tabs
                  value={tabValue}
                  onChange={handleTabChange}
                  aria-label="lower section tabs"
                  variant="fullWidth"
                >
                  <Tab label="Datasets" id="tab-0" aria-controls="tabpanel-0" />
                  <Tab label="Records" id="tab-1" aria-controls="tabpanel-1" />
                </Tabs>

                {/* Tab 1: Datasets */}
                {datasetMetadataFetchedData.loading && "Loading..."}
                {!datasetMetadataFetchedData.loading && (
                  <TabPanel value={tabValue} index={0}>
                    {datasetMetadata && datasetMetadata.length > 0 && (
                      <DatasetTab
                        datasetMetadata={datasetMetadata}
                        onDelete={(id) => {
                          // Implement your delete logic here, e.g. calling an API endpoint
                          console.log("Deleting dataset ID:", id);
                        }}
                      />
                    )}
                  </TabPanel>
                )}

                {/* Tab 2: Records */}
                {recordMetadataFetchedData.loading && "Loading..."}
                {!recordMetadataFetchedData.loading && (
                  <TabPanel value={tabValue} index={1}>
                    {recordMetadata && recordMetadata.length > 0 && (
                      <RecordTab
                        recordMetadata={recordMetadata}
                        onDelete={(id) => {
                          // Implement your delete logic here, e.g. calling an API endpoint
                          console.log("Deleting dataset ID:", id);
                        }}
                      />
                    )}
                  </TabPanel>
                )}

              </Paper>
            </Grid>
          </Box>
        </Grid>
      )}
    </>
  );
}
