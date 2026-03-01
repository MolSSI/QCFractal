// src/pages/Profile.tsx
import { usePortalClient } from "../PortalClient.tsx";
import { useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { useLocation, useParams } from "react-router-dom";
import { useAuth } from "../Auth.tsx";
import { usePreferences } from "../PreferencesProvider.tsx";
import { Star, StarBorder } from "@mui/icons-material";
import {
  Box,
  Button,
  Chip,
  Grid,
  IconButton,
  Paper,
  Tab,
  Tabs,
  Tooltip,
  Typography,
} from "@mui/material";
import DatasetTab from "../components/DatasetTab";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import RecordTab from "../components/RecordTab";
import { useQuery } from "@tanstack/react-query";
import { updateFavoritesList } from "../Utils";

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
  const location = useLocation();
  const { makeRequest } = usePortalClient();
  const { loggedIn } = useAuth();
  const { preferences, updatePreference } = usePreferences();

  const favoriteProjects = (preferences?.favorite_projects as number[]) || [];

  const handleToggleFavorite = async () => {
    if (!projectId) return;
    const projectIdNum = parseInt(projectId);
    const newFavorites = updateFavoritesList(favoriteProjects, projectIdNum);
    await updatePreference("favorite_projects", newFavorites);
  };

  // Load from location state or sessionStorage or fallback to 0
  const [tabValue, setTabValue] = useState<number>(() => {
    const saved = sessionStorage.getItem("projectTabValue");
    return location.state?.activeTab ?? (saved ? parseInt(saved) : 0);
  });

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
    sessionStorage.setItem("projectTabValue", newValue.toString());
  };

  const {
    status: projectStatus,
    data: projectData,
    error: projectError,
  } = useQuery({
    queryKey: ["project", projectId],
    queryFn: () => makeRequest<qcpTypes.Project>("GET", `api/v1/projects/${projectId}`),
    enabled: !!projectId,
  });

  const {
    status: datasetMetadataStatus,
    data: datasetMetadata,
  } = useQuery({
    queryKey: ["projectDatasetMetadata", projectId],
    queryFn: () =>
      makeRequest<Array<qcpTypes.ProjectDatasetMetadata>>(
        "GET",
        `api/v1/projects/${projectId}/dataset_metadata`,
      ),
    enabled: !!projectId,
  });

  const {
    status: recordMetadataStatus,
    data: recordMetadata,
  } = useQuery({
    queryKey: ["projectRecordMetadata", projectId],
    queryFn: () =>
      makeRequest<Array<qcpTypes.ProjectRecordMetadata>>(
        "GET",
        `api/v1/projects/${projectId}/record_metadata`,
      ),
    enabled: !!projectId,
  });

  if (!projectId) {
    return (
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
        <ErrorIndicator message="Missing project ID" />
      </Box>
    );
  }

  const isFavorite = favoriteProjects.includes(parseInt(projectId));

  return (
    <>
      {projectStatus === "pending" && (
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
          <LoadingIndicator />
        </Box>
      )}

      {projectStatus === "error" && (
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
          <ErrorIndicator message={projectError.message} />
        </Box>
      )}

      {projectStatus === "success" && projectData && (
        <Grid container spacing={2} width="100%">
          {/* Project name & tagline */}
          <Grid size={12}>
            <Box sx={{ display: "flex", alignItems: "center" }}>
              {loggedIn && (
                <Tooltip title={isFavorite ? "Remove from favorites" : "Add to favorites"}>
                  <IconButton
                    size="large"
                    onClick={handleToggleFavorite}
                    sx={{ mr: 1, p: 1 }}
                  >
                    {isFavorite ? (
                      <Star sx={{ color: "gold", fontSize: "1.5rem" }} />
                    ) : (
                      <StarBorder sx={{ fontSize: "1.5rem" }} />
                    )}
                  </IconButton>
                </Tooltip>
              )}
              <Box>
                <Typography variant="h4" fontWeight="bold">
                  <Typography variant="h5" fontWeight="bold" component={"span"} pr={2}>
                    [{projectData.id}]
                  </Typography>
                  {projectData.name}
                </Typography>
                <Typography
                  variant="subtitle1"
                  sx={{ color: "text.secondary" }}
                >
                  {projectData.tagline}
                </Typography>
              </Box>
            </Box>
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
          <Grid size={12} sx={{ mx: "auto" }}>
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
                {datasetMetadataStatus === "pending" && <LoadingIndicator />}
                {datasetMetadataStatus !== "pending" && (
                  <TabPanel value={tabValue} index={0}>
                    {datasetMetadata && datasetMetadata.length > 0 && (
                      <DatasetTab
                        datasetMetadata={datasetMetadata}
                        onDelete={(id) => {
                          console.log("Deleting dataset ID:", id);
                        }}
                      />
                    )}
                  </TabPanel>
                )}

                {/* Tab 2: Records */}
                {recordMetadataStatus === "pending" && <LoadingIndicator />}
                {recordMetadataStatus !== "pending" && (
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
          </Grid>
        </Grid>
      )}
    </>
  );
}
