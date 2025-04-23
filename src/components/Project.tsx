// src/pages/Profile.tsx
import {
  usePortalClientRequest,
  useCommonRequest,
} from "../usePortalClient.ts";
import { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes.ts";
import { useParams } from "react-router-dom";
import {
  Box,
  Button,
  Chip,
  Grid,
  Paper,
  Typography,
  Tabs,
  Tab,
} from "@mui/material";
import DatasetTab from "./DatasetTab.tsx";
import RecordTab from "./RecordTab.tsx";

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

  const { connectionState, makeRequest } = usePortalClientRequest(); // Get client instance here
  const [error, setError] = useState<string | undefined>(undefined);

  //State for metadata
  const [projectDataLoading, setProjectDataLoading] = useState(true);
  const [projectData, setProjectData] = useState<qcpTypes.Project | undefined>(
    undefined,
  );

  // dataset and record metadata
  const [datasetMetadata, setDatasetMetadata] = useState<
    Array<Record<string, unknown>>
  >([]);
  const [datasetMetadataLoading, setDatasetMetadataLoading] = useState(false);

  const [recordMetadata, setRecordMetadata] = useState<
    Array<Record<string, unknown>>
  >([]);
  const [recordMetadataLoading, setRecordMetadataLoading] = useState(false);

  // State for the Tabs
  const [tabValue, setTabValue] = useState(0);
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  // Fetch project metadata
  useEffect(() => {
    setProjectDataLoading(true);
    makeRequest<qcpTypes.Project>("get", `api/v1/projects/${projectId}`)
      .then(({ data, error }) => {
        setProjectData(data);
        setError(error);
        console.log("Project data:", data);
      })
      .catch((error) => {
        setError(error);
      })
      .finally(() => {
        setProjectDataLoading(false);
      });
  }, [projectId, connectionState, makeRequest]);

  // Fetch dataset metadata
  useEffect(() => {
    setDatasetMetadataLoading(true);
    makeRequest<Record<string, unknown>>(
      "get",
      `api/v1/projects/${projectId}/dataset_metadata`,
    )
      .then(({ data, error }) => {
        setDatasetMetadata(data);
        setError(error);
      })
      .catch((error) => {
        setError(error);
      })
      .finally(() => {
        setDatasetMetadataLoading(false);
      });
  }, [projectId, connectionState, makeRequest]);

  // Fetch record metadata
  useEffect(() => {
    setRecordMetadata(true);
    makeRequest<Record<string, unknown>>(
      "get",
      `api/v1/projects/${projectId}/record_metadata`,
    )
      .then(({ data, error }) => {
        setRecordMetadata(data);
        setError(error);
      })
      .catch((error) => {
        setError(error);
      })
      .finally(() => {
        setRecordMetadataLoading(false);
      });
  }, [projectId, connectionState, makeRequest]);

  return (
    <>
      {projectDataLoading && <Typography>Loading...</Typography>}
      {error && <Typography color="error">{error}</Typography>}

      {!projectDataLoading && projectData && (
        <Grid container spacing={2} width="100%">
          {/* Project name & tagline */}
          <Grid item xs={12}>
            <Typography variant="h4" fontWeight="bold">
              {projectData.name}
            </Typography>
            <Typography variant="subtitle1" sx={{ color: "text.secondary" }}>
              {projectData.tagline}
            </Typography>
          </Grid>
          {/* Summary stats: for example, Records, Datasets, Molecules */}
          <Grid item xs={12} sm={4}>
            <Paper elevation={3}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold">
                  {recordMetadata.length} Records
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Paper elevation={3}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold">
                  {datasetMetadata.length} Datasets
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Paper elevation={3}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold">
                  0 Molecules
                </Typography>
              </Box>
            </Paper>
          </Grid>

          <Box sx={{ width: "100%", mx: "auto" }}>
            {/* Description & Metadata section */}
            <Grid item xs={12} mb={3}>
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

            {/* Lower section with Tabs for Datasets, Records, Molecules */}
            <Grid item xs={12} mt={3}>
              <Paper elevation={2}>
                <Tabs
                  value={tabValue}
                  onChange={handleTabChange}
                  aria-label="lower section tabs"
                  variant="fullWidth"
                >
                  <Tab label="Datasets" id="tab-0" aria-controls="tabpanel-0" />
                  <Tab label="Records" id="tab-1" aria-controls="tabpanel-1" />
                  <Tab
                    label="Molecules"
                    id="tab-2"
                    aria-controls="tabpanel-2"
                  />
                </Tabs>

                {/* Tab 1: Datasets */}
                {datasetMetadataLoading && "Loading..."}
                {!datasetMetadataLoading && datasetMetadata && (
                  <TabPanel value={tabValue} index={0}>
                    {datasetMetadata.length > 0 && (
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
                {recordMetadataLoading && "Loading..."}
                {!recordMetadataLoading && (
                  <TabPanel value={tabValue} index={1}>
                    {recordMetadata.length > 0 && (
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

                {/* Tab 3: Molecules - placeholder content */}
                <TabPanel value={tabValue} index={2}>
                  <Typography variant="h6" gutterBottom>
                    Molecules
                  </Typography>
                  <Typography variant="body1">
                    Placeholder for molecule details: in the future, you could
                    display a list of molecule structures or 2D/3D
                    visualizations here.
                  </Typography>
                </TabPanel>
              </Paper>
            </Grid>
          </Box>
        </Grid>
      )}
    </>
  );
}
