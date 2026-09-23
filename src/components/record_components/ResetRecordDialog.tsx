import React from "react";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Tooltip,
} from "@mui/material";
import RestartAltIcon from "@mui/icons-material/RestartAlt";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes";
import { usePortalClient } from "../../PortalClient.tsx";
import { useAuth } from "../../Auth.tsx";
import ErrorIndicator from "../ErrorIndicator.tsx";

export interface ResetRecordDialogProps {
  recordId: number;
  open: boolean;
  onClose: () => void;
}

export const ResetRecordDialog: React.FC<ResetRecordDialogProps> = ({
  recordId,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const resetMutation = useMutation({
    mutationFn: async () => {
      const body: qcpTypes.RecordModifyBody = {
        record_ids: [recordId],
        status: "waiting",
      };

      const meta = await makeRequest<qcpTypes.UpdateMetadata>(
        "PATCH",
        "api/v1/records",
        body,
      );

      // A rejected reset comes back 200 with nothing updated
      if (meta.updated_idx.length === 0) {
        throw new Error(
          meta.errors[0]?.[1] ??
            meta.error_description ??
            "The record was not reset",
        );
      }

      return meta;
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["record", recordId] });
      onClose();
    },
  });

  const handleClose = () => {
    if (!resetMutation.isPending) {
      resetMutation.reset();
      onClose();
    }
  };

  return (
    <Dialog open={open} onClose={handleClose}>
      <DialogTitle>Reset Record</DialogTitle>
      <DialogContent>
        <DialogContentText>
          Reset record {recordId} back to waiting so that it will run again?
          This is useful if you think the error may be spurious or random. Any
          errored children of this record are reset as well.
        </DialogContentText>
        {resetMutation.isError && (
          <Box sx={{ mt: 2 }}>
            <ErrorIndicator
              message={
                (resetMutation.error as Error).message ||
                "Failed to reset record"
              }
            />
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={resetMutation.isPending}>
          Cancel
        </Button>
        <Button
          onClick={() => resetMutation.mutate()}
          variant="contained"
          autoFocus
          disabled={resetMutation.isPending}
        >
          {resetMutation.isPending ? "Resetting..." : "Reset"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export interface ResetRecordButtonProps {
  recordId: number;
  status: qcpTypes.RecordStatus;
}

export const ResetRecordButton: React.FC<ResetRecordButtonProps> = ({
  recordId,
  status,
}) => {
  const { has_permission } = useAuth();
  const [openResetDialog, setOpenResetDialog] = React.useState(false);

  // The backend only resets errored records - it accepts the request for any
  // other status and silently does nothing
  const canReset = status === "error";
  const canModify = has_permission("records", "modify");

  const tooltip = !canModify
    ? "You do not have permission to modify records"
    : canReset
      ? "Reset this record back to waiting so that it will run again"
      : "Only records in the error status can be reset";

  return (
    <>
      <Tooltip title={tooltip}>
        <span>
          <Button
            variant="outlined"
            size="small"
            startIcon={<RestartAltIcon />}
            disabled={!canReset || !canModify}
            onClick={(e) => {
              e.stopPropagation();
              setOpenResetDialog(true);
            }}
          >
            Reset
          </Button>
        </span>
      </Tooltip>

      <ResetRecordDialog
        recordId={recordId}
        open={openResetDialog}
        onClose={() => setOpenResetDialog(false)}
      />
    </>
  );
};
