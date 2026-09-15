import React from "react";
import * as qcpTypes from "../../PortalTypes";
import { PriorityEnum } from "../../PortalTypes";
import { usePortalClient } from "../../PortalClient.tsx";
import { useAuth } from "../../Auth.tsx";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  Grid,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import EditIcon from "@mui/icons-material/Edit";

// The PATCH endpoint takes the full metadata object, so fields that are not
// being edited are sent back unchanged.
function useModifyDatasetMetadata(dataset: qcpTypes.Dataset) {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (changes: Partial<qcpTypes.DatasetModifyMetadata>) =>
      makeRequest<null>(
        "PATCH",
        `api/v1/datasets/${dataset.dataset_type}/${dataset.id}`,
        {
          name: dataset.name,
          description: dataset.description ?? "",
          tagline: dataset.tagline ?? "",
          tags: dataset.tags ?? [],
          provenance: dataset.provenance ?? {},
          extras: dataset.extras ?? {},
          default_compute_tag: dataset.default_compute_tag ?? "*",
          default_compute_priority: (dataset.default_compute_priority ??
            1) as PriorityEnum,
          ...changes,
        } satisfies qcpTypes.DatasetModifyMetadata,
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dataset", dataset.id] });
      queryClient.invalidateQueries({ queryKey: ["listDatasets"] });
    },
  });
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

interface EditDatasetProps {
  dataset: qcpTypes.Dataset;
}

/**
 * Pencil icon next to the dataset name that opens a dialog for renaming it.
 */
export const EditDatasetNameButton: React.FC<EditDatasetProps> = ({
  dataset,
}) => {
  const { has_permission } = useAuth();
  const [open, setOpen] = React.useState(false);
  const [name, setName] = React.useState(dataset.name);
  const mutation = useModifyDatasetMetadata(dataset);

  if (!has_permission("datasets", "modify")) return null;

  const handleOpen = () => {
    setName(dataset.name);
    mutation.reset();
    setOpen(true);
  };

  const handleClose = () => {
    if (!mutation.isPending) {
      mutation.reset();
      setOpen(false);
    }
  };

  const handleSave = () => {
    mutation.mutate({ name: name.trim() }, { onSuccess: () => setOpen(false) });
  };

  const trimmedName = name.trim();

  return (
    <>
      <Tooltip title="Rename dataset">
        <IconButton size="small" onClick={handleOpen} sx={{ ml: 1 }}>
          <EditIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
        <DialogTitle>Rename Dataset</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {mutation.isError && (
              <Typography color="error" variant="body2">
                {errorMessage(mutation.error, "Failed to rename dataset")}
              </Typography>
            )}
            <TextField
              label="Dataset Name"
              required
              fullWidth
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
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
            variant="outlined"
            onClick={handleSave}
            disabled={
              mutation.isPending || !trimmedName || trimmedName === dataset.name
            }
          >
            {mutation.isPending ? "Saving..." : "Save"}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

/**
 * Edit button for the metadata block: description, tags, default compute tag
 * and default compute priority.
 */
export const EditDatasetMetadataButton: React.FC<EditDatasetProps> = ({
  dataset,
}) => {
  const { has_permission } = useAuth();
  const [open, setOpen] = React.useState(false);
  const [description, setDescription] = React.useState(
    dataset.description ?? "",
  );
  const [tagsText, setTagsText] = React.useState("");
  const [computeTag, setComputeTag] = React.useState(
    dataset.default_compute_tag ?? "",
  );
  // "" means "leave the priority alone" — the API requires a concrete value,
  // so a blank selection just sends the dataset's current one back.
  const [computePriority, setComputePriority] = React.useState<
    PriorityEnum | ""
  >((dataset.default_compute_priority ?? "") as PriorityEnum | "");
  const mutation = useModifyDatasetMetadata(dataset);

  if (!has_permission("datasets", "modify")) return null;

  const handleOpen = () => {
    setDescription(dataset.description ?? "");
    setTagsText((dataset.tags ?? []).join(", "));
    setComputeTag(dataset.default_compute_tag ?? "");
    setComputePriority(
      (dataset.default_compute_priority ?? "") as PriorityEnum | "",
    );
    mutation.reset();
    setOpen(true);
  };

  const handleClose = () => {
    if (!mutation.isPending) {
      mutation.reset();
      setOpen(false);
    }
  };

  const handleSave = () => {
    mutation.mutate(
      {
        description,
        tags: tagsText
          .split(",")
          .map((tag) => tag.trim())
          .filter((tag) => tag.length > 0),
        default_compute_tag: computeTag.trim(),
        // Omitted when blank, so the mutation falls back to the current value
        ...(computePriority === ""
          ? {}
          : { default_compute_priority: computePriority }),
      },
      { onSuccess: () => setOpen(false) },
    );
  };

  return (
    <>
      <Button size="small" startIcon={<EditIcon />} onClick={handleOpen}>
        Edit
      </Button>
      <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
        <DialogTitle>Edit Dataset Metadata</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {mutation.isError && (
              <Typography color="error" variant="body2">
                {errorMessage(mutation.error, "Failed to update dataset")}
              </Typography>
            )}
            <TextField
              label="Description"
              multiline
              fullWidth
              minRows={4}
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              helperText="Markdown is supported"
            />
            <TextField
              label="Tags (comma separated)"
              fullWidth
              value={tagsText}
              onChange={(e) => setTagsText(e.target.value)}
            />
            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 6 }}>
                <TextField
                  label="Default Compute Tag"
                  required
                  fullWidth
                  value={computeTag}
                  onChange={(e) => setComputeTag(e.target.value)}
                />
              </Grid>
              <Grid size={{ xs: 12, sm: 6 }}>
                <FormControl fullWidth>
                  <InputLabel shrink>Default Compute Priority</InputLabel>
                  <Select
                    displayEmpty
                    notched
                    value={computePriority}
                    label="Default Compute Priority"
                    onChange={(e) =>
                      setComputePriority(e.target.value as PriorityEnum | "")
                    }
                  >
                    <MenuItem value="">
                      <em>Leave Blank</em>
                    </MenuItem>
                    <MenuItem value={2}>High</MenuItem>
                    <MenuItem value={1}>Normal</MenuItem>
                    <MenuItem value={0}>Low</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
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
            variant="outlined"
            onClick={handleSave}
            disabled={mutation.isPending || !computeTag.trim()}
          >
            {mutation.isPending ? "Saving..." : "Save"}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};
