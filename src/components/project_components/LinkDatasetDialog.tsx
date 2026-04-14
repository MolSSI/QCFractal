import React, { useState } from "react";
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Typography,
} from "@mui/material";
import { usePortalClient } from "../../PortalClient";
import * as qcpTypes from "../../PortalTypes";
import { DatasetSearch } from "../DatasetSearch";
import { useMutation } from "@tanstack/react-query";
import ErrorIndicator from "../ErrorIndicator.tsx";

interface LinkDatasetDialogProps {
  projectId: number;
  open: boolean;
  onClose: (success?: boolean) => void;
}

const LinkDatasetDialog: React.FC<LinkDatasetDialogProps> = ({
  projectId,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();
  const [selectedDatasetId, setSelectedDatasetId] = useState<number | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: (datasetId: number) =>
      makeRequest<void>("POST", `/api/v1/projects/${projectId}/datasets/link`, {
        dataset_id: datasetId,
      } as qcpTypes.ProjectLinkDatasetBody),
    onSuccess: () => {
      onClose(true);
      setSelectedDatasetId(null);
      setError(null);
    },
    onError: (e: any) => {
      setError(e.message || "Failed to link dataset");
    },
  });

  const handleLink = () => {
    if (selectedDatasetId === null) {
      setError("Please select a dataset");
      return;
    }
    setError(null);
    mutation.mutate(selectedDatasetId);
  };

  const handleClose = () => {
    if (!mutation.isPending) {
      onClose();
      setSelectedDatasetId(null);
      setError(null);
    }
  };

  const isSubmitting = mutation.isPending;

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Link Existing Dataset</DialogTitle>
      <DialogContent>
        <Typography variant="body2" sx={{ mb: 2 }}>
          Search for an existing dataset to link to this project.
        </Typography>
        <DatasetSearch onDatasetSelect={setSelectedDatasetId} />
        {error && (
          <ErrorIndicator
            message={error || "Failed to link dataset"}
          />
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={isSubmitting}>
          Cancel
        </Button>
        <Button
          onClick={handleLink}
          disabled={selectedDatasetId === null || isSubmitting}
          variant="contained"
          color="primary"
        >
          {isSubmitting ? "Linking..." : "Link Dataset"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default LinkDatasetDialog;
