import React from "react";
import { Chip } from "@mui/material";

const roleColors: Record<string, "error" | "success" | "info" | "default"> = {
  admin: "error",
  submit: "success",
  monitor: "info",
  read: "default",
};

interface RoleChipProps {
  role: string;
  size?: "small" | "medium";
}

export const RoleChip: React.FC<RoleChipProps> = ({ role, size = "small" }) => {
  const color = roleColors[role] ?? "default";
  return <Chip label={role} color={color} size={size} sx={{ fontWeight: "bold" }} />;
};

export default RoleChip;
