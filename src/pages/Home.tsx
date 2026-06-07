import React from "react";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Divider,
  Link as MuiLink,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { useAuth } from "../Auth.tsx";
import { usePreferences } from "../PreferencesProvider.tsx";
import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "../components/LoadingIndicator";
import { usePageTitle } from "../UsePageTitle.ts";
import { RecordTypeChip } from "../components/RecordTypeChip.tsx";
import StatusChip from "../components/StatusChip.tsx";

const feedbackUrl: string = import.meta.env.VITE_FEEDBACK_URL;

const HomePage: React.FC = () => {
  usePageTitle("Home");
  const { loggedIn, has_permission } = useAuth();
  const { preferences } = usePreferences();
  const { makeRequest } = usePortalClient();

  const canFavorite = has_permission("me", "modify");

  const favoriteProjectsIds = React.useMemo(() => {
    if (!preferences?.favorite_projects) return [];
    return preferences.favorite_projects as number[];
  }, [preferences]);

  const favoriteDatasetsIds = React.useMemo(() => {
    if (!preferences?.favorite_datasets) return [];
    return preferences.favorite_datasets as number[];
  }, [preferences]);

  const favoriteRecordsIds = React.useMemo(() => {
    if (!preferences?.favorite_records) return [];
    return preferences.favorite_records as number[];
  }, [preferences]);

  const { data: projects, isLoading: projectsLoading } = useQuery({
    queryKey: ["listProjects"],
    queryFn: () =>
      makeRequest<qcpTypes.ProjectListEntry[]>("GET", "/api/v1/projects"),
    enabled: canFavorite && favoriteProjectsIds.length > 0,
  });

  const { data: datasets, isLoading: datasetsLoading } = useQuery({
    queryKey: ["listDatasets"],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetListEntry[]>("GET", "/api/v1/datasets"),
    enabled: canFavorite && favoriteDatasetsIds.length > 0,
  });

  const { data: favoriteRecords, isLoading: recordsLoading } = useQuery({
    queryKey: ["favoriteRecords", favoriteRecordsIds],
    queryFn: () =>
      makeRequest<qcpTypes.BaseRecord[]>("POST", "/api/v1/records/bulkGet", {
        ids: favoriteRecordsIds,
        include: ["record_type", "status"],
      }),
    enabled: canFavorite && favoriteRecordsIds.length > 0,
  });

  const favoriteProjects = React.useMemo(() => {
    if (!projects) return [];
    return projects.filter((p) => favoriteProjectsIds.includes(p.id));
  }, [projects, favoriteProjectsIds]);

  const favoriteDatasets = React.useMemo(() => {
    if (!datasets) return [];
    return datasets.filter((d) => favoriteDatasetsIds.includes(d.id));
  }, [datasets, favoriteDatasetsIds]);

  return (
    <Box width="100%" sx={{ p: 2 }}>
      <Alert severity="info" sx={{ mb: 3 }}>
        This webapp is under development and is still an alpha version. If you
        have any questions or comments, contact the developers
        {feedbackUrl && (
          <>
            {" "}
            or visit the{" "}
            <MuiLink
              component="a"
              href={feedbackUrl}
              target="_blank"
              rel="noopener"
            >
              feedback form
            </MuiLink>
            .
          </>
        )}
      </Alert>
      <Typography variant="h4" gutterBottom>
        Welcome to QCArchive
      </Typography>

      <Stack spacing={4}>
        {/* What's New Section */}
        <Paper sx={{ p: 3 }}>
          <Typography variant="h5" gutterBottom>
            What's New / Changelog
          </Typography>
          <Divider sx={{ mb: 2 }} />
          <Typography variant="body1" component="div">
            <ul style={{ listStyleType: "none", paddingLeft: 0 }}>
              <li>
                <Typography variant="subtitle1" sx={{ fontWeight: "bold" }}>
                  2026-06-07
                </Typography>
                <ul>
                  <li>
                    <strong>Added:</strong> API Access Page
                    dialog
                  </li>
                </ul>
              </li>
              <li>
                <Typography variant="subtitle1" sx={{ fontWeight: "bold" }}>
                  2026-06-03
                </Typography>
                <ul>
                  <li>
                    <strong>Added:</strong> Dataset relationship button and
                    dialog
                  </li>
                  <li>
                    <strong>Improved:</strong> Parent projects are now displayed
                    in the record relationship dialog
                  </li>
                </ul>
              </li>
              <li>
                <Typography variant="subtitle1" sx={{ fontWeight: "bold" }}>
                  2026-06-02
                </Typography>
                <ul>
                  <li>
                    <strong>Added:</strong> Server statistics page
                  </li>
                </ul>
              </li>

              <Accordion
                variant="outlined"
                sx={{ mt: 2, "&:before": { display: "none" } }}
              >
                <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                  <Typography sx={{ fontWeight: "bold" }}>
                    Previous Updates
                  </Typography>
                </AccordionSummary>
                <AccordionDetails sx={{ pt: 0 }}>
                  <ul style={{ listStyleType: "none", paddingLeft: 0 }}>
                    <li>
                      <Typography
                        variant="subtitle1"
                        sx={{ fontWeight: "bold" }}
                      >
                        2026-05-20
                      </Typography>
                      <ul>
                        <li>
                          <strong>Improved:</strong> Enhanced performance for
                          large output files with virtual scrolling and
                          debouncing
                        </li>
                      </ul>
                    </li>
                    <li>
                      <strong>2026-04-29</strong>
                      <ul>
                        <li>
                          <strong>Added:</strong> User management list for
                          administrators
                        </li>
                        <li>
                          <strong>Improved:</strong> Administrator profile now
                          includes extra management options
                        </li>
                      </ul>
                    </li>
                    <li style={{ marginTop: "16px" }}>
                      <strong>2026-04-28</strong>
                      <ul>
                        <li>
                          <strong>Added:</strong> Lookup by project or dataset
                          id/name
                        </li>
                        <li>
                          <strong>Added:</strong> Record relationship dialog
                        </li>
                      </ul>
                    </li>
                    <li style={{ marginTop: "16px" }}>
                      <strong>2026-04-27</strong>
                      <ul>
                        <li>
                          <strong>Added:</strong> User info page (and user
                          modification)
                        </li>
                      </ul>
                    </li>
                    <li style={{ marginTop: "16px" }}>
                      <strong>2026-04-13</strong>
                      <ul>
                        <li>
                          <strong>Added:</strong> Favoriting records
                        </li>
                        <li>
                          <strong>Added:</strong> Dataset and project
                          attachments
                        </li>
                        <li>
                          <strong>Added:</strong> Project creation
                        </li>
                        <li>
                          <strong>Added:</strong> Linking/Unlinking existing
                          datasets to a project
                        </li>
                        <li>
                          <strong>Improved:</strong> Enhanced
                          record/dataset/project description display with
                          Markdown support
                        </li>
                        <li>
                          <strong>Added:</strong> Torsiondrive plots
                        </li>
                        <li>
                          <strong>Improved:</strong> Manager page & fragment
                          (including claimed records)
                        </li>
                        <li>
                          <strong>Improved:</strong> Remove ANSI escape codes
                          from raw output
                        </li>
                      </ul>
                    </li>
                    <li style={{ marginTop: "16px" }}>
                      <strong>2026-04-10</strong>
                      <ul>
                        <li>
                          <strong>Improved:</strong> Molecular formula
                          formatting & molecule viewer layouts
                        </li>
                        <li>
                          <strong>Improved:</strong> Remove ANSI escape codes
                          from raw output
                        </li>
                      </ul>
                    </li>
                    <li style={{ marginTop: "16px" }}>
                      <strong>2026-04-09</strong>
                      <ul>
                        <li>
                          <strong>Added:</strong> This homepage{" "}
                        </li>
                        <li>
                          <strong>Added:</strong> Dataset records and various
                          record pages
                        </li>
                      </ul>
                    </li>
                  </ul>
                </AccordionDetails>
              </Accordion>
            </ul>
          </Typography>
        </Paper>

        {loggedIn && (
          <>
            {/* Favorite Projects Section */}
            <Box>
              <Typography variant="h5" gutterBottom>
                Favorite Projects
              </Typography>
              {favoriteProjectsIds.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  You don't have any favorite any projects.
                </Typography>
              ) : projectsLoading ? (
                <LoadingIndicator />
              ) : (
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell width="10%">ID</TableCell>
                        <TableCell>Project Name</TableCell>
                        <TableCell>Owner</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {favoriteProjects.map((project) => (
                        <TableRow key={project.id} hover>
                          <TableCell>
                            <Typography fontWeight="bold">
                              {project.id}
                            </Typography>
                          </TableCell>
                          <TableCell>
                            <MuiLink
                              to={`/projects/${project.id}`}
                              sx={{
                                color: "inherit",
                                fontWeight: "bold",
                                textDecoration: "none",
                                "&:hover": { textDecoration: "underline" },
                              }}
                            >
                              {project.project_name}
                            </MuiLink>
                            <Typography variant="body2" color="text.secondary">
                              {project.tagline}
                            </Typography>
                          </TableCell>
                          <TableCell>{project.owner_user}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Box>

            {/* Favorite Datasets Section */}
            <Box>
              <Typography variant="h5" gutterBottom>
                Favorite Datasets
              </Typography>
              {favoriteDatasetsIds.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  You don't have any favorite any datasets.
                </Typography>
              ) : datasetsLoading ? (
                <LoadingIndicator />
              ) : (
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell width="10%">ID</TableCell>
                        <TableCell>Dataset Name</TableCell>
                        <TableCell>Type</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {favoriteDatasets.map((dataset) => (
                        <TableRow key={dataset.id} hover>
                          <TableCell>
                            <Typography fontWeight="bold">
                              {dataset.id}
                            </Typography>
                          </TableCell>
                          <TableCell>
                            <MuiLink
                              to={`/datasets/${dataset.id}`}
                              sx={{
                                color: "inherit",
                                fontWeight: "bold",
                                textDecoration: "none",
                                "&:hover": { textDecoration: "underline" },
                              }}
                            >
                              {dataset.dataset_name}
                            </MuiLink>
                            <Typography variant="body2" color="text.secondary">
                              {dataset.tagline}
                            </Typography>
                          </TableCell>
                          <TableCell>
                            <RecordTypeChip type={dataset.dataset_type} />
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Box>

            {/* Favorite Records Section */}
            <Box>
              <Typography variant="h5" gutterBottom>
                Favorite Records
              </Typography>
              {favoriteRecordsIds.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  You don't have any favorite any records.
                </Typography>
              ) : recordsLoading ? (
                <LoadingIndicator />
              ) : (
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell width="10%">ID</TableCell>
                        <TableCell>Type</TableCell>
                        <TableCell>Status</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {favoriteRecords?.map((record) => (
                        <TableRow key={record.id} hover>
                          <TableCell>
                            <MuiLink
                              to={`/records/${record.id}`}
                              sx={{
                                color: "inherit",
                                fontWeight: "bold",
                                textDecoration: "none",
                                "&:hover": { textDecoration: "underline" },
                              }}
                            >
                              {record.id}
                            </MuiLink>
                          </TableCell>
                          <TableCell>
                            <RecordTypeChip type={record.record_type} />
                          </TableCell>
                          <TableCell>
                            <StatusChip
                              status={record.status}
                              recordId={record.id}
                              recordType={record.record_type}
                            />
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Box>
          </>
        )}
      </Stack>
    </Box>
  );
};

export default HomePage;
