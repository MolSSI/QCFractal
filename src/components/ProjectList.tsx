import React from "react";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  Box,
  Chip,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";

const ProjectList: React.FC = () => {
  const navigate = useNavigate();
  const { makeRequest } = usePortalClient();

  const handleClick = (projectId: string) => {
    navigate(`/projects/${projectId}`);
  };

  const {
    status,
    data: projects,
    error,
  } = useQuery({
    queryKey: ["listProjects"],
    queryFn: () => makeRequest<qcpTypes.Project[]>("GET", "/api/v1/projects"),
  });

  if (status == "pending") {
    return <Typography>Loading...</Typography>;
  }

  if (status == "error") {
    return <Typography color="error">{error.message}</Typography>;
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
