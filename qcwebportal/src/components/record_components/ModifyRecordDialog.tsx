import React from "react";
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  FormControl,
  Grid,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
} from "@mui/material";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes";
import { usePortalClient } from "../../PortalClient.tsx";
import ErrorIndicator from "../ErrorIndicator.tsx";
import LoadingIndicator from "../LoadingIndicator";
import { describeRequestError, formatPriority } from "../../Utils.ts";

type QueueEntry = qcpTypes.RecordTask | qcpTypes.RecordService;

export interface ModifyRecordDialogProps {
  recordId: number;
  recordType: string;
  isService: boolean;
  open: boolean;
  onClose: () => void;
}

export const ModifyRecordDialog: React.FC<ModifyRecordDialogProps> = ({
  recordId,
  recordType,
  isService,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  // Compute tag and priority live on the record's task (or service), which the
  // record page does not fetch. The endpoint returns null once the record has
  // left the queue - completed records have no task to retag
  const entryType = isService ? "service" : "task";
  const {
    status: entryStatus,
    data: queueEntry,
    error: entryError,
  } = useQuery({
    queryKey: ["recordTask", recordType, recordId],
    queryFn: () =>
      makeRequest<QueueEntry | null>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/${entryType}`,
      ),
    enabled: open,
  });

  // The read side names these tag/priority on current servers and compute_tag/
  // compute_priority on newer ones; the write side is always compute_*
  const currentTag = queueEntry?.compute_tag ?? queueEntry?.tag ?? "";
  const currentPriority =
    queueEntry?.compute_priority ?? queueEntry?.priority ?? 1;

  const [comment, setComment] = React.useState("");
  const [computeTag, setComputeTag] = React.useState("");
  const [computePriority, setComputePriority] =
    React.useState<qcpTypes.PriorityEnum>(1);

  // Seed the fields once the queue entry arrives, and again whenever the dialog
  // is reopened
  React.useEffect(() => {
    if (!open) return;
    setComment("");
    setComputeTag(currentTag);
    setComputePriority(currentPriority as qcpTypes.PriorityEnum);
  }, [open, currentTag, currentPriority]);

  const inQueue = queueEntry != null;
  const trimmedComment = comment.trim();
  const trimmedTag = computeTag.trim();
  // The server lowercases tags, so a case-only edit is not a change
  const tagChanged =
    inQueue && trimmedTag.toLowerCase() !== currentTag.toLowerCase();
  const priorityChanged = inQueue && computePriority !== currentPriority;
  const hasChanges = trimmedComment.length > 0 || tagChanged || priorityChanged;

  const modifyMutation = useMutation({
    mutationFn: async () => {
      const patch = async (body: qcpTypes.RecordModifyBody, failed: string) => {
        const meta = await makeRequest<qcpTypes.UpdateMetadata>(
          "PATCH",
          "api/v1/records",
          body,
        );
        // A rejected change still comes back 200 with nothing updated
        if (meta.updated_idx.length === 0) {
          throw new Error(
            meta.errors[0]?.[1] ?? meta.error_description ?? failed,
          );
        }
      };

      // One concern per request: the backend overwrites its return value as it
      // walks status -> tag/priority -> comment, so a combined call could only
      // report on the comment
      if (tagChanged || priorityChanged) {
        await patch(
          {
            record_ids: [recordId],
            ...(tagChanged ? { compute_tag: trimmedTag } : {}),
            ...(priorityChanged ? { compute_priority: computePriority } : {}),
          },
          "The compute tag and priority were not changed",
        );
      }

      if (trimmedComment.length > 0) {
        await patch(
          { record_ids: [recordId], comment: trimmedComment },
          "The comment was not added",
        );
      }
    },
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["record", recordId] }),
        queryClient.invalidateQueries({
          queryKey: ["recordTask", recordType, recordId],
        }),
      ]);
      onClose();
    },
  });

  const handleClose = () => {
    if (!modifyMutation.isPending) {
      modifyMutation.reset();
      onClose();
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>Modify Record {recordId}</DialogTitle>
      <DialogContent>
        {entryStatus === "pending" && <LoadingIndicator />}
        {entryStatus === "error" && (
          <ErrorIndicator
            message={describeRequestError(
              entryError,
              "Could not read the record's task",
            )}
          />
        )}
        {entryStatus === "success" && (
          <Stack spacing={2} sx={{ mt: 1 }}>
            {modifyMutation.isError && (
              <ErrorIndicator
                message={describeRequestError(
                  modifyMutation.error,
                  "Failed to modify the record",
                )}
              />
            )}

            <DialogContentText variant="body2">
              Comments are append-only and are attributed to you. Changing the
              compute tag or priority only affects work that has not run yet.
            </DialogContentText>

            <TextField
              label="Add a comment"
              multiline
              fullWidth
              minRows={3}
              value={comment}
              onChange={(e) => setComment(e.target.value)}
              helperText="Left blank, no comment is added"
            />

            {!inQueue && (
              <Alert severity="info">
                This record is not in the compute queue, so it has no compute
                tag or priority to change.
              </Alert>
            )}

            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 6 }}>
                <TextField
                  label="Compute Tag"
                  fullWidth
                  disabled={!inQueue}
                  value={computeTag}
                  onChange={(e) => setComputeTag(e.target.value)}
                  helperText="Only managers with a matching tag pick this up; the server lowercases it"
                />
              </Grid>
              <Grid size={{ xs: 12, sm: 6 }}>
                <FormControl fullWidth disabled={!inQueue}>
                  <InputLabel>Compute Priority</InputLabel>
                  <Select
                    value={computePriority}
                    label="Compute Priority"
                    onChange={(e) =>
                      setComputePriority(
                        e.target.value as qcpTypes.PriorityEnum,
                      )
                    }
                  >
                    <MenuItem value={0}>{formatPriority(0)}</MenuItem>
                    <MenuItem value={1}>{formatPriority(1)}</MenuItem>
                    <MenuItem value={2}>{formatPriority(2)}</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Stack>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={modifyMutation.isPending}>
          Cancel
        </Button>
        <Button
          onClick={() => modifyMutation.mutate()}
          variant="contained"
          disabled={!hasChanges || modifyMutation.isPending}
        >
          {modifyMutation.isPending ? "Saving..." : "Save changes"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ModifyRecordDialog;
