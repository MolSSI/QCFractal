import React from "react";
import { Chip, Tooltip, Dialog, DialogContent, Box, IconButton } from "@mui/material";
import HelpOutline from "@mui/icons-material/HelpOutline";
import ErrorOutline from "@mui/icons-material/ErrorOutline";
import CloseIcon from "@mui/icons-material/Close";
import { WaitingReasonFragment } from "./WaitingReasonFragment";
import { ViewOutputDialog } from "./ViewOutputDialog.tsx";

interface StatusProps {
  status: string;
  recordType?: string;
  recordId?: number;
  computeHistoryId?: number;
}

const statusColors: Record<
  string,
  | "success"
  | "error"
  | "warning"
  | "default"
  | "info"
  | "primary"
  | "secondary"
> = {
  complete: "success",
  error: "error",
  waiting: "warning",
  invalid: "default",
  running: "info",
  cancelled: "primary",
  deleted: "secondary",
};

export const StatusChip: React.FC<StatusProps> = ({ status, recordType, recordId, computeHistoryId}) => {
  const [waitingReasonOpen, setWaitingReasonOpen] = React.useState(false);
  const [errorDetailsOpen, setErrorDetailsOpen] = React.useState(false);

  const lowerStatus = status.toLowerCase();
  const color = statusColors[lowerStatus] || "default";

  const showWaitingReason = lowerStatus === "waiting" && recordId !== undefined;
  const showError = lowerStatus === "error" && recordId !== undefined && recordType !== undefined;

  return (
    <Box display="flex" alignItems="center" gap={1}>
      <Chip
        label={status}
        color={color}
        sx={{ fontWeight: "bold", textTransform: "capitalize" }}
      />

      {showWaitingReason && (
        <>
          <Tooltip title="Click here for waiting reason">
            <HelpOutline
              fontSize="small"
              color="action"
              sx={{ cursor: "pointer" }}
              onClick={(e) => {
                e.stopPropagation();
                setWaitingReasonOpen(true);
              }}
            />
          </Tooltip>
          <Dialog
            fullWidth={true}
            open={waitingReasonOpen}
            onClose={() => {
              setWaitingReasonOpen(false);
            }}
            onClick={(e) => {
              e.stopPropagation();
            }}
          >
            <DialogContent sx={{ position: "relative", pt: 5 }}>
              <IconButton
                size="small"
                onClick={() => setWaitingReasonOpen(false)}
                sx={{
                  position: "absolute",
                  right: 8,
                  top: 8,
                  "&&": { bgcolor: "transparent", border: "none" },
                  "&&:hover": { bgcolor: "transparent", border: "none" },
                  "&&:active": { bgcolor: "transparent" },
                }}
              >
                <CloseIcon fontSize="small" />
              </IconButton>
              <WaitingReasonFragment recordId={recordId} />
            </DialogContent>
          </Dialog>
        </>
      )}
      {showError && (
        <>
          <Tooltip title="Click here to view error">
            <ErrorOutline
              fontSize="small"
              color="action"
              sx={{ cursor: "pointer" }}
              onClick={(e) => {
                e.stopPropagation();
                setErrorDetailsOpen(true);
              }}
            />
          </Tooltip>
          <ViewOutputDialog
            recordId={recordId}
            recordType={recordType}
            computeHistoryId={computeHistoryId}
            initialKey={"error"}
            open={errorDetailsOpen}
            onClose={() => {
              setErrorDetailsOpen(false);
            }}
            />
        </>
      )}
    </Box>
  );
};

export default StatusChip;
