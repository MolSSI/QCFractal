import React from "react";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Divider,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Tooltip,
} from "@mui/material";
import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import EditIcon from "@mui/icons-material/Edit";
import BlockIcon from "@mui/icons-material/Block";
import CancelOutlinedIcon from "@mui/icons-material/CancelOutlined";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import UndoIcon from "@mui/icons-material/Undo";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes";
import { usePortalClient } from "../../PortalClient.tsx";
import { useAuth } from "../../Auth.tsx";
import ErrorIndicator from "../ErrorIndicator.tsx";
import { ModifyRecordDialog } from "./ModifyRecordDialog.tsx";

type ActionKind = "cancel" | "invalidate" | "delete" | "revert";

type ActionDefinition = {
  /** Label in the menu / on the button */
  label: string;
  icon: React.ReactNode;
  /** resource/action pair checked against the user's role */
  permission: [string, string];
  /** Statuses the backend accepts this action from */
  appliesTo: qcpTypes.RecordStatus[];
  /** One sentence shown when the action is offered but not usable */
  unavailable: string;
  /** Sentence shown on hover when the action *is* usable */
  available: string;
  /** What the user is about to cause, shown in the confirmation dialog */
  consequence: string;
  confirmLabel: string;
  pendingLabel: string;
  destructive?: boolean;
};

// Source statuses come from record_socket.py - see .claude/record-actions-api.md.
// Sending an action from any other status is accepted with a 200 and quietly
// does nothing, so the menu has to be the gate.
const ACTIONS: Record<Exclude<ActionKind, "revert">, ActionDefinition> = {
  cancel: {
    label: "Cancel",
    icon: <CancelOutlinedIcon fontSize="small" />,
    permission: ["records", "modify"],
    appliesTo: ["waiting", "running", "error"],
    unavailable:
      "Only records that are waiting, running, or errored can be cancelled.",
    available: "Take this record out of the compute queue without deleting it",
    consequence:
      "Cancelling takes this record out of the compute queue so it will not run, " +
      "and cancels every child record it owns. Any work currently in progress on a " +
      "manager is discarded. You can undo this later with Revert.",
    confirmLabel: "Cancel record",
    pendingLabel: "Cancelling...",
  },
  invalidate: {
    label: "Invalidate",
    icon: <BlockIcon fontSize="small" />,
    permission: ["records", "modify"],
    appliesTo: ["complete"],
    unavailable: "Only completed records can be invalidated.",
    available: "Mark this completed record as untrustworthy",
    consequence:
      "Invalidating marks this completed record as untrustworthy, so its results " +
      "are treated as unusable wherever it appears. The computed data is kept and " +
      "child records are left alone. You can undo this later with Revert.",
    confirmLabel: "Invalidate",
    pendingLabel: "Invalidating...",
  },
  delete: {
    label: "Delete",
    icon: <DeleteOutlineIcon fontSize="small" />,
    permission: ["records", "delete"],
    appliesTo: [
      "waiting",
      "running",
      "error",
      "complete",
      "cancelled",
      "invalid",
    ],
    unavailable: "This record has already been deleted.",
    available: "Remove this record and its children from the server listings",
    consequence:
      "Deleting removes this record, and every child record it owns, from dataset " +
      "and project listings. This is a soft delete - the data stays on the server " +
      "and you can undo it with Revert - but anything referencing the record will " +
      "stop showing it.",
    confirmLabel: "Delete",
    pendingLabel: "Deleting...",
    destructive: true,
  },
};

// Revert undoes the last cancel/invalidate/delete, so which one it undoes is
// determined by the status the record is sitting in now. It restores the
// record's *previous* status, which is not necessarily waiting.
const REVERTIBLE: Partial<
  Record<
    qcpTypes.RecordStatus,
    { verb: string; available: string; consequence: string }
  >
> = {
  cancelled: {
    verb: "Uncancel",
    available: "Undo the cancellation of this record",
    consequence:
      "This undoes the cancellation and puts the record, along with its children, " +
      "back into the status it had before it was cancelled - so a record that was " +
      "waiting or running goes back into the compute queue.",
  },
  invalid: {
    verb: "Uninvalidate",
    available: "Mark this record as valid again",
    consequence:
      "This undoes the invalidation and restores the record, along with its " +
      "children, to the status it had before - its results count as valid again.",
  },
  deleted: {
    verb: "Undelete",
    available: "Restore this deleted record",
    consequence:
      "This restores the record and its children to the status they held before " +
      "being deleted - an errored record comes back as errored, not as waiting - " +
      "and they reappear in dataset and project listings.",
  },
};

const REVERT_UNAVAILABLE =
  "There is no cancellation, invalidation, or deletion on this record to undo.";

export interface RecordActionsMenuProps {
  recordId: number;
  recordType: string;
  isService: boolean;
  status: qcpTypes.RecordStatus;
}

export const RecordActionsMenu: React.FC<RecordActionsMenuProps> = ({
  recordId,
  recordType,
  isService,
  status,
}) => {
  const { makeRequest } = usePortalClient();
  const { has_permission } = useAuth();
  const queryClient = useQueryClient();

  const [anchorEl, setAnchorEl] = React.useState<HTMLElement | null>(null);
  const [pendingAction, setPendingAction] = React.useState<ActionKind | null>(
    null,
  );
  const [modifyOpen, setModifyOpen] = React.useState(false);

  const revert = REVERTIBLE[status];

  const actionMutation = useMutation({
    mutationFn: async (kind: ActionKind) => {
      if (kind === "delete") {
        const body: qcpTypes.RecordDeleteBody = {
          record_ids: [recordId],
          soft_delete: true,
          delete_children: true,
        };
        const meta = await makeRequest<qcpTypes.DeleteMetadata>(
          "POST",
          "api/v1/records/bulkDelete",
          body,
        );
        // A rejected action still comes back 200 with nothing changed
        if (meta.deleted_idx.length === 0) {
          throw new Error(
            meta.errors[0]?.[1] ??
              meta.error_description ??
              "The record was not deleted",
          );
        }
        return;
      }

      let meta: qcpTypes.UpdateMetadata;
      if (kind === "revert") {
        const body: qcpTypes.RecordRevertBody = {
          record_ids: [recordId],
          // The status being undone, not the one being restored
          revert_status: status,
        };
        meta = await makeRequest<qcpTypes.UpdateMetadata>(
          "POST",
          "api/v1/records/revert",
          body,
        );
      } else {
        const body: qcpTypes.RecordModifyBody = {
          record_ids: [recordId],
          status: kind === "cancel" ? "cancelled" : "invalid",
        };
        meta = await makeRequest<qcpTypes.UpdateMetadata>(
          "PATCH",
          "api/v1/records",
          body,
        );
      }

      if (meta.updated_idx.length === 0) {
        throw new Error(
          meta.errors[0]?.[1] ??
            meta.error_description ??
            "The record was not changed",
        );
      }
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["record", recordId] });
      setPendingAction(null);
    },
  });

  const closeMenu = () => setAnchorEl(null);

  const handleSelect = (kind: ActionKind) => {
    closeMenu();
    actionMutation.reset();
    setPendingAction(kind);
  };

  const handleCloseDialog = () => {
    if (!actionMutation.isPending) {
      actionMutation.reset();
      setPendingAction(null);
    }
  };

  const describe = (kind: Exclude<ActionKind, "revert">) => {
    const action = ACTIONS[kind];
    const allowed = has_permission(...action.permission);
    const applicable = action.appliesTo.includes(status);
    return {
      action,
      enabled: allowed && applicable,
      tooltip: !allowed
        ? `You do not have permission to ${action.permission[1]} records.`
        : applicable
          ? action.available
          : action.unavailable,
    };
  };

  // Modify is not status-gated the way the state changes are - a comment can go
  // on any record - but editing one that is hidden from every listing is not
  // useful, so deleted records have to be restored first
  const canModifyAttributes =
    has_permission("records", "modify") && status !== "deleted";
  const modifyTooltip = !has_permission("records", "modify")
    ? "You do not have permission to modify records."
    : status === "deleted"
      ? "Undelete this record before changing its comments, tag, or priority."
      : "Add a comment, or change the compute tag and priority";

  const canRevert = has_permission("records", "modify") && revert !== undefined;
  const revertTooltip = !has_permission("records", "modify")
    ? "You do not have permission to modify records."
    : revert
      ? revert.available
      : REVERT_UNAVAILABLE;

  // Dialog copy for whichever action is awaiting confirmation
  const dialogCopy =
    pendingAction === null
      ? null
      : pendingAction === "revert"
        ? {
            title: `${revert?.verb ?? "Revert"} Record`,
            consequence: revert?.consequence ?? "",
            confirmLabel: revert?.verb ?? "Revert",
            pendingLabel: "Reverting...",
            destructive: false,
          }
        : {
            title: `${ACTIONS[pendingAction].label} Record`,
            consequence: ACTIONS[pendingAction].consequence,
            confirmLabel: ACTIONS[pendingAction].confirmLabel,
            pendingLabel: ACTIONS[pendingAction].pendingLabel,
            destructive: ACTIONS[pendingAction].destructive ?? false,
          };

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        endIcon={<ArrowDropDownIcon />}
        onClick={(e) => {
          e.stopPropagation();
          setAnchorEl(e.currentTarget);
        }}
      >
        Actions
      </Button>

      <Menu anchorEl={anchorEl} open={anchorEl !== null} onClose={closeMenu}>
        <Tooltip title={modifyTooltip} placement="right">
          <span style={{ display: "block" }}>
            <MenuItem
              disabled={!canModifyAttributes}
              onClick={() => {
                closeMenu();
                setModifyOpen(true);
              }}
            >
              <ListItemIcon>
                <EditIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText>Modify...</ListItemText>
            </MenuItem>
          </span>
        </Tooltip>

        <Divider />

        {(["cancel", "invalidate", "delete"] as const).map((kind) => {
          const { action, enabled, tooltip } = describe(kind);
          return (
            // Every action is always listed so the menu does not shift around;
            // a disabled MenuItem swallows hover events, hence the span
            <Tooltip key={kind} title={tooltip} placement="right">
              <span style={{ display: "block" }}>
                <MenuItem
                  disabled={!enabled}
                  onClick={() => handleSelect(kind)}
                  sx={action.destructive ? { color: "error.main" } : undefined}
                >
                  <ListItemIcon
                    sx={
                      action.destructive ? { color: "error.main" } : undefined
                    }
                  >
                    {action.icon}
                  </ListItemIcon>
                  <ListItemText>{action.label}</ListItemText>
                </MenuItem>
              </span>
            </Tooltip>
          );
        })}

        <Divider />

        <Tooltip title={revertTooltip} placement="right">
          <span style={{ display: "block" }}>
            <MenuItem
              disabled={!canRevert}
              onClick={() => handleSelect("revert")}
            >
              <ListItemIcon>
                <UndoIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText>Revert</ListItemText>
            </MenuItem>
          </span>
        </Tooltip>
      </Menu>

      <ModifyRecordDialog
        recordId={recordId}
        recordType={recordType}
        isService={isService}
        open={modifyOpen}
        onClose={() => setModifyOpen(false)}
      />

      <Dialog open={dialogCopy !== null} onClose={handleCloseDialog}>
        <DialogTitle>{dialogCopy?.title}</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Record {recordId} is currently in the {status} status.{" "}
            {dialogCopy?.consequence}
          </DialogContentText>
          {actionMutation.isError && (
            <Box sx={{ mt: 2 }}>
              <ErrorIndicator
                message={
                  (actionMutation.error as Error).message || "The action failed"
                }
              />
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button
            onClick={handleCloseDialog}
            disabled={actionMutation.isPending}
          >
            Never mind
          </Button>
          <Button
            onClick={() =>
              pendingAction && actionMutation.mutate(pendingAction)
            }
            variant="contained"
            color={dialogCopy?.destructive ? "error" : "primary"}
            autoFocus
            disabled={actionMutation.isPending}
          >
            {actionMutation.isPending
              ? dialogCopy?.pendingLabel
              : dialogCopy?.confirmLabel}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default RecordActionsMenu;
