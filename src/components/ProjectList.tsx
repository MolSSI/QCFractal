import React, { useEffect, useState } from "react";
import { usePortalClientRequest } from "../usePortalClient";
import { useNavigate } from "react-router-dom";
import {
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  TableContainer,
  Typography,
  Paper,
  Box,
  Chip,
} from "@mui/material";

interface Project {
  id: string;
  project_name: string;
  tagline: string;
  tags: string[];
  record_count: number;
  dataset_count: number;
  molecule_count: number;
}

const ProjectList: React.FC = () => {
  const { makeRequest } = usePortalClientRequest();
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>("");
  const navigate = useNavigate();

  useEffect(() => {
    async function fetchProjects() {
      setLoading(true);
      const { data, error } = await makeRequest<Project[]>(
        "GET",
        "/api/v1/projects"
      );
      if (error) {
        setError(error);
      } else if (data) {
        setProjects(data);
      }
      setLoading(false);
    }
    fetchProjects();
  }, [makeRequest]);

  const handleClick = (projectId: string) => {
    navigate(`/projects/${projectId}`);
  };

  return (
    <>
      {loading && <Typography>Loading...</Typography>}
      {error && <Typography color="error">{error}</Typography>}

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
                    <Typography variant="body1" fontWeight="bold">
                      {project.project_name}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {project.tagline}
                    </Typography>
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
