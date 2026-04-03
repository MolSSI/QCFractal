import React from "react";
import { Chip, Tooltip, Dialog, DialogContent, Box } from "@mui/material";
import HelpOutline from "@mui/icons-material/HelpOutline";
import { WaitingReasonFragment } from "./WaitingReasonFragment";

interface StatusProps {
  status: string;
  recordId?: number;
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

export const StatusChip: React.FC<StatusProps> = ({ status, recordId }) => {
  const [waitingReasonOpen, setWaitingReasonOpen] = React.useState(false);

  const lowerStatus = status.toLowerCase();
  const color = statusColors[lowerStatus] || "default";

  return (
    <Box display="flex" alignItems="center" gap={1}>
      <Chip
        label={status}
        color={color}
        sx={{ fontWeight: "bold", textTransform: "capitalize" }}
      />
      {lowerStatus === "waiting" && recordId !== undefined && (
        <>
          <Tooltip title="Click here for waiting reason">
            <HelpOutline
              fontSize="small"
              color="action"
              sx={{ cursor: "pointer" }}
              onClick={() => setWaitingReasonOpen(true)}
            />
          </Tooltip>
          <Dialog
            fullWidth={true}
            open={waitingReasonOpen}
            onClose={() => setWaitingReasonOpen(false)}
          >
            <DialogContent>
              <WaitingReasonFragment recordId={recordId} />
            </DialogContent>
          </Dialog>
        </>
      )}
    </Box>
  );
};

export default StatusChip;
