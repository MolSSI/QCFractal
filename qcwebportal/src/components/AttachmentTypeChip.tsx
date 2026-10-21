import React from "react";
import { Chip } from "@mui/material";

interface AttachmentTypeChipProps {
  type: string;
}

export const AttachmentTypeChip: React.FC<AttachmentTypeChipProps> = ({ type }) => {
  return (
    <Chip
      label={type}
      color="primary"
      variant="outlined"
      size="small"
      sx={{ fontWeight: "bold" }}
    />
  );
};

export default AttachmentTypeChip;
