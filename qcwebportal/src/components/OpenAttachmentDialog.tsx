import React from "react";
import { IconButton, Tooltip } from "@mui/material";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import { DatasetAttachment, ProjectAttachment } from "../PortalTypes";

type Attachment = DatasetAttachment | ProjectAttachment;

export interface OpenAttachmentButtonProps {
  attachment: Attachment;
  disabled?: boolean;
}

export const OpenAttachmentButton: React.FC<OpenAttachmentButtonProps> = ({
  attachment: _attachment,
  disabled = false,
}) => {
  return (
    <Tooltip title="Open">
      <span>
        <IconButton size="small" disabled={disabled}>
          <OpenInNewIcon fontSize="small" />
        </IconButton>
      </span>
    </Tooltip>
  );
};
