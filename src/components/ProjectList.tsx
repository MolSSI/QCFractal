import React, { useEffect, useState } from "react";
import { usePortalClientRequest } from "../usePortalClient";
import { useNavigate } from "react-router-dom";
import {
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  CircularProgress,
  Alert,
} from "@mui/material";

interface Project {
  id: string;
  project_name: string;
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
        "/api/v1/projects",
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

  if (loading) {
    return <CircularProgress />;
  }

  if (error) {
    return <Alert severity="error">{error}</Alert>;
  }

  return (
    <>
      <h1>Projects</h1>
      <List>
        {projects.map((project) => (
          <ListItem key={project.id} disablePadding>
            <ListItemButton onClick={() => handleClick(project.id)}>
              <ListItemText primary={project.project_name} />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
    </>
  );
};

export default ProjectList;
