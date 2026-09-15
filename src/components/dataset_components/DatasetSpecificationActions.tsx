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
