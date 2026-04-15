import React from "react";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  Box,
  Chip,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Typography,
  Button,
  IconButton,
  Collapse,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { FavoriteButton } from "../components/FavoriteButton.tsx";
import { useAuth } from "../Auth.tsx";
import { AddProjectButton } from "../components/project_components/AddProjectDialog";
import ReactMarkdown from "react-markdown";

const ProjectRow: React.FC<{ project: qcpTypes.ProjectListEntry }> = ({
  project,
}) => {
  const { makeRequest } = usePortalClient();
  const [isExpanded, setIsExpanded] = React.useState(false);

  const {
    status,
    data: projectDetails,
    error,
  } = useQuery({
    queryKey: ["project", project.id],
    queryFn: () =>
      makeRequest<qcpTypes.Project>("GET", `/api/v1/projects/${project.id}`),
    enabled: isExpanded,
  });

  const handleToggleExpand = () => {
    setIsExpanded((prev) => !prev);
  };

  return (
    <React.Fragment>
      <TableRow
        key={project.id}
        hover
        onClick={handleToggleExpand}
        sx={{ cursor: "pointer" }}
      >
        <TableCell width="5%">
          <IconButton size="small">
            {isExpanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell>
          <Stack direction="row" spacing={1} alignItems="center">
            <FavoriteButton
              preferencesKey="favorite_projects"
              objectId={project.id}
            />
            <Typography fontWeight={"bold"}>{project.id}</Typography>
          </Stack>
        </TableCell>
        <TableCell>
          <Box sx={{ display: "flex", alignItems: "center" }}>
            <Box>
              <Typography variant="body2" fontWeight="bold">
                {project.project_name}
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
              <Chip key={tag} label={tag} size="small" variant="outlined" />
            ))}
          </Box>
        </TableCell>
        <TableCell>
          <Button
            variant="contained"
            size="small"
            component={Link}
            to={`/projects/${project.id}`}
            onClick={(e) => {
              e.stopPropagation();
            }}
          >
            View
          </Button>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={7}>
          <Collapse in={isExpanded} timeout="auto" unmountOnExit>
            <Box m={3} display="flex" flexDirection="column" gap={2}>
              {status === "pending" && <LoadingIndicator />}
              {status === "error" && <ErrorIndicator message={error.message} />}
              {status === "success" && projectDetails && (
                <>
                  {projectDetails.description && (
                    <Box>
                      <Typography variant="body2" fontWeight="bold">
                        Description
                      </Typography>
                      <Typography variant="body2" component="div">
                        <ReactMarkdown>
                          {projectDetails.description.trim()}
                        </ReactMarkdown>
                      </Typography>
                    </Box>
                  )}
                  {!projectDetails.description && (
                    <Box>
                      <Typography variant="body2" fontWeight="bold">
                        Description
                      </Typography>
                      <Typography variant="body2" component="div">
                        No description available for this dataset.
                      </Typography>
                    </Box>
                  )}
                </>
              )}
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

const ProjectList: React.FC = () => {
  const { makeRequest } = usePortalClient();
  const { has_permission } = useAuth();

  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(20);
  const [filter, setFilter] = React.useState("");

  const canAddProject = has_permission("projects", "add");

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
    return projects.filter(
      (project) =>
        project.project_name.toLowerCase().includes(filter.toLowerCase()) ||
        project.id.toString().includes(filter),
    );
  }, [projects, filter]);

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
        <Stack
          direction="row"
          spacing={2}
          alignItems="center"
          justifyContent="space-between"
          sx={{ mb: 2, width: "100%" }}
        >
          <Box width={"30%"}>
            <TextField
              fullWidth
              variant="outlined"
              size="small"
              label="Filter projects"
              value={filter}
              onChange={handleFilterChange}
            />
          </Box>
          <Box>{canAddProject && <AddProjectButton />}</Box>
        </Stack>
        <TableContainer component={Paper} variant="outlined">
          <Table size="medium">
            <TableHead>
              <TableRow>
                <TableCell width="50px" />
                <TableCell width="5%">ID</TableCell>
                <TableCell width="45%">Project Name</TableCell>
                <TableCell width="10%">Owner</TableCell>
                <TableCell width="10%">Content</TableCell>
                <TableCell width="20%">Tags</TableCell>
                <TableCell width="10%">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredProjects
                .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                .map((project) => (
                  <ProjectRow key={project.id} project={project} />
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
