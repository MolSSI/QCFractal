import React from "react";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  Tooltip,
  Typography,
} from "@mui/material";
import DeleteIcon from "@mui/icons-material/Delete";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { DatasetAttachment, ProjectAttachment } from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { dateStringToLocalTime, formatSize } from "../Utils.ts";
import ErrorIndicator from "./ErrorIndicator.tsx";

type Attachment = DatasetAttachment | ProjectAttachment;

export interface DeleteAttachmentDialogProps {
  attachment: Attachment;
  parentType: "dataset" | "project";
  parentId: number;
  parentName: string;
  open: boolean;
  onClose: () => void;
}

export const DeleteAttachmentDialog: React.FC<DeleteAttachmentDialogProps> = ({
  attachment,
  parentType,
  parentId,
  parentName,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();
  const attachmentQueryKey =
    parentType === "project"
      ? ["projectAttachments", parentId]
      : ["datasetAttachments", parentId];

  const deleteMutation = useMutation({
    mutationFn: async () => {
      const requestPath =
        parentType === "project"
          ? `api/v1/projects/${parentId}/attachments/${attachment.id}`
          : `api/v1/datasets/${parentId}/attachments/${attachment.id}`;

      await makeRequest("DELETE", requestPath);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: attachmentQueryKey,
      });
      onClose();
    },
  });

  const handleClose = () => {
    if (!deleteMutation.isPending) {
      deleteMutation.reset();
      onClose();
    }
  };

  const handleConfirmDelete = () => {
    deleteMutation.mutate();
  };

  return (
    <Dialog open={open} onClose={handleClose}>
      <DialogTitle>Confirm Delete</DialogTitle>
      <DialogContent>
        <DialogContentText>
          Are you sure you want to delete the following attachment?
        </DialogContentText>
        <Box sx={{ mt: 2, p: 2, bgcolor: "action.hover", borderRadius: 1 }}>
          <Typography variant="body2">
            <strong>Attached to:</strong> {parentName}
          </Typography>
          <Typography variant="body2">
            <strong>Filename:</strong> {attachment.file_name}
          </Typography>
          <Typography variant="body2">
            <strong>Size:</strong> {formatSize(attachment.file_size)}
          </Typography>
          <Typography variant="body2">
            <strong>Created On:</strong>{" "}
            {dateStringToLocalTime(attachment.created_on)}
          </Typography>
        </Box>
        {deleteMutation.isError && (
          <Box sx={{ mt: 2 }}>
            <ErrorIndicator
              message={
                (deleteMutation.error as Error).message ||
                "Failed to delete attachment"
              }
            />
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={deleteMutation.isPending}>
          Cancel
        </Button>
        <Button
          onClick={handleConfirmDelete}
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

export interface DeleteAttachmentButtonProps {
  attachment: Attachment;
  parentType: "dataset" | "project";
  parentId: number;
  parentName: string;
  disabled?: boolean;
}

export const DeleteAttachmentButton: React.FC<DeleteAttachmentButtonProps> = ({
  attachment,
  parentType,
  parentId,
  parentName,
  disabled = false,
}) => {
  const [openDeleteDialog, setOpenDeleteDialog] = React.useState(false);

  return (
    <>
      <Tooltip title="Delete">
        <span>
          <IconButton
            size="small"
            disabled={disabled}
            onClick={() => setOpenDeleteDialog(true)}
          >
            <DeleteIcon fontSize="small" />
          </IconButton>
        </span>
      </Tooltip>
      <DeleteAttachmentDialog
        attachment={attachment}
        parentType={parentType}
        parentId={parentId}
        parentName={parentName}
        open={openDeleteDialog}
        onClose={() => setOpenDeleteDialog(false)}
      />
    </>
  );
};
