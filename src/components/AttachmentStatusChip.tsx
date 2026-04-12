import React from "react";
import { Chip } from "@mui/material";

interface AttachmentStatusChipProps {
  status: string;
}

const statusColors: Record<
  string,
  | "success"
  | "warning"
> = {
  available: "success",
  processing: "warning",
};

export const AttachmentStatusChip: React.FC<AttachmentStatusChipProps> = ({
  status,
}) => {
  const lowerStatus = status.toLowerCase();
  const color = statusColors[lowerStatus] || "default";

  return (
    <Chip
      label={status}
      color={color}
      size="small"
      sx={{ fontWeight: "bold", textTransform: "capitalize" }}
    />
  );
};

export default AttachmentStatusChip;
