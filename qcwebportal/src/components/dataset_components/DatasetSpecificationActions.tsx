import React from "react";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  FormControlLabel,
  Radio,
  RadioGroup,
  TextField,
  Typography,
} from "@mui/material";
import {
  QueryClient,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes";
import { usePortalClient } from "../../PortalClient.tsx";
import { describeRequestError } from "../../Utils.ts";
import ErrorIndicator from "../ErrorIndicator.tsx";

// The specification a record belongs to is part of every dataset-scoped list,
// so refetch them all once the server has reshuffled things.
function invalidateSpecificationQueries(
  queryClient: QueryClient,
  datasetType: qcpTypes.RecordType,
  datasetId: number,
) {
  return Promise.all(
    [
      ["datasetSpecifications", datasetType, datasetId],
      ["datasetStatus", datasetType, datasetId],
      ["datasetRecordCount", datasetType, datasetId],
      ["datasetRecordDiscovery", datasetType, datasetId],
    ].map((queryKey) => queryClient.invalidateQueries({ queryKey })),
  );
}

/**
 * Renames a specification. The records attached to it are carried over to the
 * new name by the server, so nothing else has to be updated here.
 */
export function useRenameSpecification(
  datasetType: qcpTypes.RecordType,
  datasetId: number,
) {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  return useMutation({
    // The endpoint takes a map of old name -> new name
    mutationFn: ({ oldName, newName }: { oldName: string; newName: string }) =>
      makeRequest<null>(
        "PATCH",
        `api/v1/datasets/${datasetType}/${datasetId}/specifications`,
        { [oldName]: newName },
      ),
    onSuccess: () =>
      invalidateSpecificationQueries(queryClient, datasetType, datasetId),
  });
}

/**
 * Sets the description of a specification. An empty description clears it.
 */
export function useModifySpecificationDescription(
  datasetType: qcpTypes.RecordType,
  datasetId: number,
) {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      name,
      description,
    }: {
      name: string;
      description: string;
    }) =>
      makeRequest<null>(
        "PATCH",
        `api/v1/datasets/${datasetType}/${datasetId}/specifications/modify`,
        { name, description: description.trim() === "" ? null : description },
      ),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: ["datasetSpecifications", datasetType, datasetId],
      }),
  });
}

interface EditSpecificationDialogProps {
  datasetType: qcpTypes.RecordType;
  datasetId: number;
  specName: string;
  description: string;
  existingNames: string[];
  onRenamed: (oldName: string, newName: string) => void;
  onClose: () => void;
}

/**
 * Edits the name and description of a specification. These are two separate
 * server calls; the rename goes first so the description update can use the
 * new name.
 */
export const EditSpecificationDialog: React.FC<
  EditSpecificationDialogProps
> = ({
  datasetType,
  datasetId,
  specName,
  description,
  existingNames,
  onRenamed,
  onClose,
}) => {
  const renameMutation = useRenameSpecification(datasetType, datasetId);
  const descriptionMutation = useModifySpecificationDescription(
    datasetType,
    datasetId,
  );
  const [name, setName] = React.useState(specName);
  const [newDescription, setNewDescription] = React.useState(description);
  // After a rename succeeds, later retries must not rename again
  const [currentName, setCurrentName] = React.useState(specName);
  const [error, setError] = React.useState<string | null>(null);
  const [saving, setSaving] = React.useState(false);

  const trimmedName = name.trim();
  const nameChanged = trimmedName !== currentName;
  const descriptionChanged = newDescription !== description;
  const isDuplicateName = nameChanged && existingNames.includes(trimmedName);
  const canSave =
    trimmedName !== "" &&
    !isDuplicateName &&
    (nameChanged || descriptionChanged) &&
    !saving;

  const handleClose = () => {
    if (!saving) {
      onClose();
    }
  };

  const save = async () => {
    if (!canSave) {
      return;
    }

    setSaving(true);
    setError(null);
    let workingName = currentName;

    try {
      if (nameChanged) {
        await renameMutation.mutateAsync({
          oldName: currentName,
          newName: trimmedName,
        });
        onRenamed(currentName, trimmedName);
        workingName = trimmedName;
        setCurrentName(trimmedName);
      }
    } catch (e) {
      setError(describeRequestError(e, "Failed to rename specification"));
      setSaving(false);
      return;
    }

    try {
      if (descriptionChanged) {
        await descriptionMutation.mutateAsync({
          name: workingName,
          description: newDescription,
        });
      }
    } catch (e) {
      setError(
        describeRequestError(e, "Failed to update description") +
          (workingName !== specName ? " (the name was changed)" : ""),
      );
      setSaving(false);
      return;
    }

    setSaving(false);
    onClose();
  };

  return (
    <Dialog open onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Edit Specification</DialogTitle>
      <DialogContent>
        <TextField
          autoFocus
          fullWidth
          margin="dense"
          label="Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          error={isDuplicateName}
          helperText={
            isDuplicateName
              ? "A specification with that name already exists"
              : undefined
          }
        />
        <TextField
          fullWidth
          multiline
          minRows={4}
          maxRows={20}
          margin="dense"
          label="Description"
          value={newDescription}
          onChange={(e) => setNewDescription(e.target.value)}
        />
        {error && (
          <Box sx={{ mt: 2 }}>
            <ErrorIndicator message={error} />
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={saving}>
          Cancel
        </Button>
        <Button onClick={save} variant="contained" disabled={!canSave}>
          {saving ? "Saving..." : "Save"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

interface DeleteSpecificationDialogProps {
  datasetType: qcpTypes.RecordType;
  datasetId: number;
  specName: string;
  /** Undefined while the dataset status is still loading */
  recordCount: number | undefined;
  open: boolean;
  onClose: () => void;
}

export const DeleteSpecificationDialog: React.FC<
  DeleteSpecificationDialogProps
> = ({ datasetType, datasetId, specName, recordCount, open, onClose }) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();
  const [deleteRecords, setDeleteRecords] = React.useState(false);

  const attachedRecordsLabel =
    recordCount === undefined ? "Unknown" : recordCount.toLocaleString();

  // Only name a figure once we actually have one to name
  const recordsPhrase =
    recordCount === undefined || recordCount === 0
      ? "the records"
      : `the ${attachedRecordsLabel} ${recordCount === 1 ? "record" : "records"}`;

  const deleteMutation = useMutation({
    mutationFn: () =>
      makeRequest<unknown>(
        "POST",
        `api/v1/datasets/${datasetType}/${datasetId}/specifications/bulkDelete`,
        { names: [specName], delete_records: deleteRecords },
      ),
    onSuccess: async () => {
      await invalidateSpecificationQueries(queryClient, datasetType, datasetId);
      onClose();
    },
  });

  const handleClose = () => {
    if (!deleteMutation.isPending) {
      deleteMutation.reset();
      setDeleteRecords(false);
      onClose();
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Delete Specification</DialogTitle>
      <DialogContent>
        <DialogContentText>
          Are you sure you want to delete this specification? This cannot be
          undone.
        </DialogContentText>
        <Box sx={{ mt: 2, p: 2, bgcolor: "action.hover", borderRadius: 1 }}>
          <Typography variant="body2">
            <strong>Specification:</strong> {specName}
          </Typography>
          <Typography variant="body2">
            <strong>Attached records:</strong> {attachedRecordsLabel}
          </Typography>
        </Box>

        <Typography variant="subtitle2" fontWeight="bold" sx={{ mt: 2 }}>
          What should happen to the records computed with it?
        </Typography>
        <Typography variant="body2" sx={{ color: "text.secondary" }}>
          Either way {recordsPhrase} leave this dataset along with the
          specification, so its record count drops. The choice is whether they
          stay on the server.
        </Typography>
        <RadioGroup
          value={deleteRecords ? "delete" : "keep"}
          onChange={(e) => setDeleteRecords(e.target.value === "delete")}
        >
          <FormControlLabel
            value="keep"
            control={<Radio />}
            label={`Keep ${recordsPhrase} on the server`}
          />
          <FormControlLabel
            value="delete"
            control={<Radio />}
            label={`Delete ${recordsPhrase} from the server too`}
          />
        </RadioGroup>

        {deleteMutation.isError && (
          <Box sx={{ mt: 2 }}>
            <ErrorIndicator
              message={describeRequestError(
                deleteMutation.error,
                "Failed to delete specification",
              )}
            />
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={deleteMutation.isPending}>
          Cancel
        </Button>
        <Button
          onClick={() => deleteMutation.mutate()}
          color="error"
          variant="contained"
          disabled={deleteMutation.isPending}
        >
          {deleteMutation.isPending ? "Deleting..." : "Delete"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
