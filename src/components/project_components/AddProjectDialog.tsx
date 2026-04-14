import React from "react";
import * as qcpTypes from "../../PortalTypes";
import { usePortalClient } from "../../PortalClient.tsx";
import { useNavigate } from "react-router-dom";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { PriorityEnum } from "../../PortalTypes";

interface AddProjectDialogProps {
  open: boolean;
  onClose: () => void;
}

const AddProjectDialog: React.FC<AddProjectDialogProps> = ({ open, onClose }) => {
  const { makeRequest } = usePortalClient();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [newProject, setNewProject] = React.useState<qcpTypes.ProjectAddBody>({
    name: "",
    description: "",
    tagline: "",
    tags: [],
    default_compute_tag: "*",
    default_compute_priority: 1,
    extras: {},
  });

  const mutation = useMutation({
    mutationFn: (project: qcpTypes.ProjectAddBody) =>
      makeRequest<number>("POST", "/api/v1/projects", project),
    onSuccess: (projectId) => {
      queryClient.invalidateQueries({ queryKey: ["listProjects"] });
      onClose();
      navigate(`/projects/${projectId}`);
    },
  });

  const handleAddProject = () => {
    mutation.mutate(newProject);
  };

  const handleClose = () => {
    if (!mutation.isPending) {
      onClose();
    }
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle>Add New Project</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {mutation.isError && (
            <Typography color="error" variant="body2">
              {(mutation.error as any).message || "Failed to create project"}
            </Typography>
          )}
          <TextField
            label="Project Name"
            required
            fullWidth
            value={newProject.name}
            onChange={(e) =>
              setNewProject({ ...newProject, name: e.target.value })
            }
          />
          <TextField
            label="Tagline"
            required
            fullWidth
            value={newProject.tagline}
            onChange={(e) =>
              setNewProject({ ...newProject, tagline: e.target.value })
            }
          />
          <TextField
            label="Description"
            required
            multiline
            fullWidth
            minRows={1}
            value={newProject.description}
            onChange={(e) =>
              setNewProject({ ...newProject, description: e.target.value })
            }
          />
          <TextField
            label="Tags (comma separated)"
            required
            fullWidth
            value={newProject.tags.join(", ")}
            onChange={(e) =>
              setNewProject({
                ...newProject,
                tags: e.target.value.split(",").map((s) => s.trim()),
              })
            }
            helperText="Enter tags separated by commas"
          />
          <TextField
            label="Default Compute Tag"
            required
            fullWidth
            value={newProject.default_compute_tag}
            onChange={(e) =>
              setNewProject({
                ...newProject,
                default_compute_tag: e.target.value,
              })
            }
          />
          <FormControl fullWidth required>
            <InputLabel>Default Compute Priority</InputLabel>
            <Select
              value={newProject.default_compute_priority}
              label="Default Compute Priority"
              onChange={(e) =>
                setNewProject({
                  ...newProject,
                  default_compute_priority: e.target.value as PriorityEnum,
                })
              }
            >
              <MenuItem value={2}>High</MenuItem>
              <MenuItem value={1}>Normal</MenuItem>
              <MenuItem value={0}>Low</MenuItem>
            </Select>
          </FormControl>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button
          variant="outlined"
          onClick={handleClose}
          disabled={mutation.isPending}
        >
          Cancel
        </Button>
        <Button
          onClick={handleAddProject}
          variant="outlined"
          disabled={mutation.isPending || !newProject.name || !newProject.tagline}
        >
          {mutation.isPending ? "Creating..." : "Create Project"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default AddProjectDialog;
