import React from "react";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../Auth.tsx";
import { usePreferences } from "../PreferencesProvider.tsx";
import { Star, StarBorder } from "@mui/icons-material";
import {
  Box,
  Chip,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import { updateFavoritesList } from "../Utils.ts";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";

const ProjectList: React.FC = () => {
  const navigate = useNavigate();
  const { makeRequest } = usePortalClient();
  const { has_permission } = useAuth();
  const { preferences, updatePreference } = usePreferences();

  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(20);
  const [filter, setFilter] = React.useState("");

  const favoriteProjects = (preferences?.favorite_projects as number[]) || [];

  const {
    status,
    data: projects,
    error,
  } = useQuery({
    queryKey: ["listProjects"],
    queryFn: () =>
      makeRequest<qcpTypes.ProjectListEntry[]>("GET", "/api/v1/projects"),
  });

  const filteredProjects = React.useMemo(() => {
    if (!projects) return [];
    return projects.filter((project) =>
      project.project_name.toLowerCase().includes(filter.toLowerCase()),
    );
  }, [projects, filter]);

  const handleClick = (projectId: string) => {
    navigate(`/projects/${projectId}`);
  };

  const handleToggleFavorite = async (
    event: React.MouseEvent,
    projectId: string,
  ) => {
    event.stopPropagation();
    const projectIdNum = parseInt(projectId);
    const newFavorites = updateFavoritesList(favoriteProjects, projectIdNum);
    await updatePreference("favorite_projects", newFavorites);
  };

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleFilterChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setFilter(event.target.value);
    setPage(0);
  };

  const canFavorite = has_permission("me", "modify");

  if (status == "pending") {
    return <LoadingIndicator fullPage />;
  }

  if (status == "error") {
    return <ErrorIndicator fullPage message={error.message} />;
  }

  return (
    <>
      {/* Title / Heading */}
      <Box sx={{ mt: 4, mb: 2, width: "100%" }}>
        <Typography variant="h4" gutterBottom>
          Projects
        </Typography>
      </Box>

      {/* Table wrapped in Paper for typical MUI look */}
      <Box sx={{ width: "100%" }}>
        <Box sx={{ mb: 2 }} width={"30%"}>
          <TextField
            fullWidth
            variant="outlined"
            size="small"
            label="Filter projects"
            value={filter}
            onChange={handleFilterChange}
          />
        </Box>
        <TableContainer component={Paper} variant="outlined">
          <Table size="medium">
            <TableHead>
              <TableRow>
                <TableCell width="55%">
                  <strong>Project Name</strong>
                </TableCell>
                <TableCell width="15%">
                  <strong>Owner</strong>
                </TableCell>
                <TableCell width="20%">
                  <strong>Content</strong>
                </TableCell>
                <TableCell width="10%">
                  <strong>Tags</strong>
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredProjects
                .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                .map((project) => (
                  <TableRow
                    key={project.id}
                    hover
                    sx={{ cursor: "pointer" }}
                    onClick={() => handleClick(project.id)}
                  >
                    <TableCell>
                      <Box sx={{ display: "flex", alignItems: "center" }}>
                        {canFavorite && (
                          <Tooltip
                            title={
                              favoriteProjects.includes(parseInt(project.id))
                                ? "Remove from favorites"
                                : "Add to favorites"
                            }
                          >
                            <IconButton
                              size="small"
                              onClick={(e) =>
                                handleToggleFavorite(e, project.id)
                              }
                              sx={{ mr: 1 }}
                            >
                              {favoriteProjects.includes(
                                parseInt(project.id),
                              ) ? (
                                <Star sx={{ color: "gold" }} />
                              ) : (
                                <StarBorder />
                              )}
                            </IconButton>
                          </Tooltip>
                        )}
                        <Box>
                          <Typography variant="body2" fontWeight="bold">
                            [{project.id}] {project.project_name}
                          </Typography>
                          <Typography variant="body2" color="text.secondary">
                            {project.tagline}
                          </Typography>
                        </Box>
                      </Box>
                    </TableCell>
                    <TableCell>{project.owner_user}</TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {project.record_count} records
                      </Typography>
                      <Typography variant="body2">
                        {project.dataset_count} datasets
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
                        {project.tags.map((tag) => (
                          <Chip
                            key={tag}
                            label={tag}
                            size="small"
                            variant="outlined"
                          />
                        ))}
                      </Box>
                    </TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          rowsPerPageOptions={[10, 20, 50]}
          component="div"
          count={filteredProjects.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
          sx={{ width: "100%" }}
        />
      </Box>
    </>
  );
};

export default ProjectList;
