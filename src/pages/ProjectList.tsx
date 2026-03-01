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
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import { updateFavoritesList } from "../Utils.ts";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";

const ProjectList: React.FC = () => {
  const navigate = useNavigate();
  const { makeRequest } = usePortalClient();
  const { loggedIn } = useAuth();
  const { preferences, updatePreference } = usePreferences();

  const favoriteProjects = (preferences?.favorite_projects as number[]) || [];

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

  const {
    status,
    data: projects,
    error,
  } = useQuery({
    queryKey: ["listProjects"],
    queryFn: () => makeRequest<qcpTypes.ProjectListEntry[]>("GET", "/api/v1/projects"),
  });

  if (status == "pending") {
    return (
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
        <LoadingIndicator />
      </Box>
    );
  }

  if (status == "error") {
    return (
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh", width: "100%" }}>
        <ErrorIndicator message={error.message} />
      </Box>
    );
  }

  return (
    <>
      {/* Title / Heading */}
      <Box sx={{ mt: 4, mb: 2 }}>
        <Typography variant="h4" gutterBottom>
          Projects
        </Typography>
      </Box>

      {/* Table wrapped in Paper for typical MUI look */}
      <Paper sx={{ mb: 4 }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: "bold" }}>Project Name</TableCell>
                <TableCell sx={{ fontWeight: "bold" }}>Content</TableCell>
                <TableCell sx={{ fontWeight: "bold" }}>Tags</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {projects.map((project) => (
                <TableRow
                  key={project.id}
                  hover
                  sx={{ cursor: "pointer" }}
                  onClick={() => handleClick(project.id)}
                >
                  <TableCell>
                    <Box sx={{ display: "flex", alignItems: "center" }}>
                      {loggedIn && (
                        <Tooltip title={favoriteProjects.includes(parseInt(project.id)) ? "Remove from favorites" : "Add to favorites"}>
                          <IconButton
                            size="small"
                            onClick={(e) => handleToggleFavorite(e, project.id)}
                            sx={{ mr: 1 }}
                          >
                            {favoriteProjects.includes(parseInt(project.id)) ? (
                              <Star sx={{ color: "gold" }} />
                            ) : (
                              <StarBorder />
                            )}
                          </IconButton>
                        </Tooltip>
                      )}
                      <Box>
                        <Typography variant="body1" fontWeight="bold">
                          {project.project_name}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          {project.tagline}
                        </Typography>
                      </Box>
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {project.record_count} records
                    </Typography>
                    <Typography variant="body2">
                      {project.dataset_count} datasets
                    </Typography>
                  </TableCell>
                  <TableCell>
                    {project.tags.map((tag) => (
                      <Chip
                        key={tag}
                        label={tag}
                        size="small"
                        sx={{ backgroundColor: "grey.300", mr: 1 }}
                      />
                    ))}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
    </>
  );
};

export default ProjectList;
