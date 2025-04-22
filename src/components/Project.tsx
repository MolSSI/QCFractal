// src/pages/Profile.tsx
import { usePortalClientRequest } from "../usePortalClient.ts";
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
  Tab
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

  //State for metadata
  const [data, setData] = useState<qcpTypes.Project | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | undefined>(undefined);
  const [projectData, setProjectData] = useState<any | undefined>(undefined);  
  // State for dataset metadata
  const [datasetMetadata, setDatasetMetadata] = useState<any[]>([]);

  // State for record metadata
  const [recordMetadata, setRecordMetadata] = useState<any[]>([]);

  // State for the Tabs
  const [tabValue, setTabValue] = useState(0);
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  // Fetch project metadata
  useEffect(() => {
    async function fetchData() {
      setLoading(true);
      const { data, error } = await makeRequest<qcpTypes.Project>(
        "get",
        `api/v1/projects/${projectId}`
      );
      setData(data);
      setError(error);
      setLoading(false);
    }
    fetchData();
  }, [projectId, connectionState, makeRequest]);


// Fetch project content 
  useEffect(() => {
    async function fetchProjects() {
      setLoading(true);
      const { data, error } = await makeRequest<any[]>(
        "get",
        "/api/v1/projects"
      );   
      const selectedProject = data ? data.find((project) => project.id === Number(projectId)) : {};
      setProjectData(selectedProject);
      setError(error);
      setLoading(false);
      console.log("Project data:", selectedProject);
    }
    fetchProjects();
  }, [projectId, connectionState, makeRequest]);

  // Fetch dataset metadata
  useEffect(() => {
    async function fetchDatasetMetadata() {
      setLoading(true);
      const { data, error } = await makeRequest<any[]>(
        "get",
        `api/v1/projects/${projectId}/dataset_metadata`
      );
      setDatasetMetadata(data ?? []);
      setError(error);
      setLoading(false);
    }
    fetchDatasetMetadata();
  }, [projectId, connectionState, makeRequest]);

  // Fetch record metadata
  useEffect(() => {
    async function fetchRecordMetadata() {
      setLoading(true);
      const { data, error } = await makeRequest<any[]>(
        "get",
        `api/v1/projects/${projectId}/record_metadata`
      );
      setRecordMetadata(data ?? []);
      setError(error);
      setLoading(false);
    }
    fetchRecordMetadata();
  }, [projectId, connectionState, makeRequest]);

  return (
    <>
      {loading && <Typography>Loading...</Typography>}
      {error && <Typography color="error">{error}</Typography>}

      {data && (
        <Grid container spacing={2} width="100%">
          {/* Project name & tagline */}
          <Grid item xs={12}>
            <Typography variant="h4" fontWeight="bold">
              {data.name}
            </Typography>
            <Typography variant="subtitle1" sx={{ color: "text.secondary" }}>
              {data.tagline}
            </Typography>
          </Grid>
          {/* Summary stats: for example, Records, Datasets, Molecules */}
          <Grid item xs={12} sm={4}>
            <Paper elevation={3}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold">
                  {projectData?.record_count} Records
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Paper elevation={3}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold">
                  {projectData?.dataset_count} Datasets
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Paper elevation={3}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold">
                  {projectData?.molecule_count} Molecules
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
                    {data.description}
                  </Typography>

                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Tags
                  </Typography>
                  <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
                    {data.tags?.map((tag, idx) => (
                      <Chip key={idx} label={tag} variant="outlined" />
                    ))}
                  </Box>

                  <Box mt={2}>
                    <Typography variant="h6" fontWeight="bold" gutterBottom>
                      Owner
                    </Typography>
                    <Typography variant="body1">
                      {data.owner_user || "N/A"}
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

                {/* Tab 2: Records */}
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
