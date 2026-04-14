// src/pages/Profile.tsx
import { usePortalClient } from "../PortalClient.tsx";
import React, { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { useLocation, useParams } from "react-router-dom";
import { Box, Chip, Grid, Paper, Tab, Tabs, Typography } from "@mui/material";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import ProjectDatasetTable from "../components/project_components/ProjectDatasetTable";
import ProjectRecordTable from "../components/project_components/ProjectRecordTable";
import AttachmentTable from "../components/AttachmentTable";
import { useQuery } from "@tanstack/react-query";
import { FavoriteButton } from "../components/FavoriteButton.tsx";
import ReactMarkdown from "react-markdown";

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

  useEffect(() => {
    if (projectData) {
      document.title = `Project ${projectId}: ${projectData.name}`;
    }
  }, [projectData, projectId]);

  const {
    status: datasetMetadataStatus,
    data: datasetMetadata,
    refetch: refetchDatasets,
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
  
  const {
    status: attachmentsStatus,
    data: attachmentsData,
  } = useQuery({
    queryKey: ["projectAttachments", projectId],
    queryFn: () =>
      makeRequest<Array<qcpTypes.ProjectAttachment>>(
        "GET",
        `api/v1/projects/${projectId}/attachments`,
      ),
    enabled: !!projectId && tabValue === 2,
  });

  if (!projectId) {
    return <ErrorIndicator fullPage message="Missing project ID" />;
  }

  return (
    <>
      {projectStatus === "pending" && <LoadingIndicator fullPage />}

      {projectStatus === "error" && (
        <ErrorIndicator fullPage message={projectError.message} />
      )}

      {projectStatus === "success" && projectData && (
        <Grid container spacing={2} width="100%">
          {/* Project name & tagline */}
          <Grid size={12}>
            <Box sx={{ display: "flex", alignItems: "center" }}>
              <FavoriteButton
                preferencesKey="favorite_projects"
                objectId={parseInt(projectId)}
              />
              <Box>
                <Typography variant="h4" fontWeight="bold">
                  <Typography
                    variant="h5"
                    fontWeight="bold"
                    component={"span"}
                    pr={2}
                  >
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
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {attachmentsData ? attachmentsData.length : "..."} Attachments
                </Typography>
              </Box>
            </Paper>
          </Grid>

          {/* Description & Metadata section */}
          <Grid size={12} sx={{ mx: "auto" }}>
            <Grid size={12} mb={3}>
              <Paper elevation={2}>
                <Box p={2}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Description
                  </Typography>
                  <Typography variant="body1" component={"p"}>
                    <ReactMarkdown>{projectData.description.trim()}</ReactMarkdown>
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
                  <Tab label="Attachments" id="tab-2" aria-controls="tabpanel-2" />
                </Tabs>

                {/* Tab 1: Datasets */}
                <TabPanel value={tabValue} index={0}>
                  {datasetMetadataStatus === "pending" && <LoadingIndicator />}
                  {datasetMetadataStatus === "success" && datasetMetadata && (
                    <ProjectDatasetTable
                      projectId={parseInt(projectId)}
                      datasetMetadata={datasetMetadata}
                      onDelete={(id) => {
                        console.log("Deleting dataset ID:", id);
                      }}
                      onRefresh={refetchDatasets}
                    />
                  )}
                </TabPanel>

                {/* Tab 2: Records */}
                <TabPanel value={tabValue} index={1}>
                  {recordMetadataStatus === "pending" && <LoadingIndicator />}
                  {recordMetadataStatus === "success" && recordMetadata && (
                    <ProjectRecordTable recordMetadata={recordMetadata} />
                  )}
                </TabPanel>

                {/* Tab 3: Attachments */}
                <TabPanel value={tabValue} index={2}>
                  {attachmentsStatus === "pending" && <LoadingIndicator />}
                  {attachmentsStatus === "success" && attachmentsData && (
                    <AttachmentTable attachments={attachmentsData} projectId={projectId} />
                  )}
                </TabPanel>
              </Paper>
            </Grid>
          </Grid>
        </Grid>
      )}
    </>
  );
}
