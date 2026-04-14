import React from "react";
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Typography,
} from "@mui/material";
import { usePortalClient } from "../../PortalClient";
import * as qcpTypes from "../../PortalTypes";
import { useMutation, useQueryClient } from "@tanstack/react-query";

interface UnlinkDatasetDialogProps {
  projectId: number;
  dataset: qcpTypes.ProjectDatasetMetadata | null;
  open: boolean;
  onClose: (success?: boolean) => void;
}

const UnlinkDatasetDialog: React.FC<UnlinkDatasetDialogProps> = ({
  projectId,
  dataset,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: (datasetId: number) => {
      const body: qcpTypes.ProjectUnlinkLinkDatasetBody = {
        dataset_ids: [datasetId],
        delete_datasets: false,
        delete_dataset_records: false,
      };
      return makeRequest(
        "POST",
        `/api/v1/projects/${projectId}/datasets/unlink`,
        body,
      );
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ["projectDatasetMetadata", projectId],
      });
      onClose(true);
    },
  });

  const handleConfirm = () => {
    if (dataset) {
      mutation.mutate(dataset.dataset_id);
    }
  };

  const handleClose = () => {
    if (!mutation.isPending) {
      onClose();
      mutation.reset();
    }
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      aria-labelledby="unlink-dialog-title"
      aria-describedby="unlink-dialog-description"
    >
      <DialogTitle id="unlink-dialog-title">Unlink Dataset</DialogTitle>
      <DialogContent>
        <DialogContentText id="unlink-dialog-description">
          Are you sure you want to unlink the dataset "{dataset?.name}" from
          this project? This will not delete the dataset itself.
        </DialogContentText>
        {mutation.isError && (
          <Typography color="error" variant="body2" sx={{ mt: 2 }}>
            Error:{" "}
            {(mutation.error as any)?.message || "Failed to unlink dataset"}
          </Typography>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={mutation.isPending}>
          Cancel
        </Button>
        <Button
          onClick={handleConfirm}
          color="error"
          autoFocus
          disabled={mutation.isPending || !dataset}
        >
          {mutation.isPending ? "Unlinking..." : "Unlink"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default UnlinkDatasetDialog;
