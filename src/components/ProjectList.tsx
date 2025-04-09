import React, { useEffect, useState } from "react";
import { usePortalClientRequest } from "../usePortalClient";
import { useNavigate } from "react-router-dom";
import {
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Link,
  TableContainer,
  Typography,
  Paper,
  Box,
  Chip
} from "@mui/material";

interface Project {
  id: string;
  project_name: string;
  tagline: string;
  tags: string[];
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
                <TableCell>ID</TableCell>
                <TableCell>Project Name</TableCell>
                <TableCell>Tagline</TableCell>
                <TableCell>Tags</TableCell>
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
                    <Link
                      component="button"
                      variant="body2"
                      underline="none"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleClick(project.id);
                      }}
                    >
                      {project.id}
                    </Link>
                  </TableCell>
                  <TableCell>{project.project_name}</TableCell>
                  <TableCell>{project.tagline}</TableCell>
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
