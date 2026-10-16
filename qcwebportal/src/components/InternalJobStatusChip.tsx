import React from "react";
import { Chip } from "@mui/material";
import { InternalJobStatusEnum } from "../portal_types/common.ts";

interface InternalJobStatusChipProps {
  status: InternalJobStatusEnum;
}
const statusColors: Record<
  string,
  "success" | "warning"| "info"| "error"| "primary"| "secondary"
> = {
  complete: "success",
  waiting: "warning",
  running: "info",
  error: "error",
  cancelled: "primary",
  deleted: "secondary",
};

export const InternalJobStatusChip: React.FC<InternalJobStatusChipProps> = ({ status }) => {
  const color = statusColors[status] || "default";
  return <Chip label={status} color={color} sx={{ fontWeight: "bold" }} />;
};

export default InternalJobStatusChip
