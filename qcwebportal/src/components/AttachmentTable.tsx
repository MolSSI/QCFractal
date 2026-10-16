import React, { useState } from "react";
import {
  Box,
  Collapse,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import DownloadIcon from "@mui/icons-material/Download";
import { DatasetAttachment, ProjectAttachment } from "../PortalTypes";
import AttachmentStatusChip from "./AttachmentStatusChip";
import AttachmentTypeChip from "./AttachmentTypeChip";
import { dateStringToLocalTime, formatSize } from "../Utils";
import { GenericDataList } from "./GenericDataList";
import { server_address } from "../request_config";
import { useAuth } from "../Auth.tsx";
import { DeleteAttachmentButton } from "./DeleteAttachmentDialog.tsx";
import { OpenAttachmentButton } from "./OpenAttachmentDialog.tsx";

interface AttachmentTableProps {
  attachments: (DatasetAttachment | ProjectAttachment)[];
  parentType: "dataset" | "project";
  parentId: number;
  parentName: string;
}

function AttachmentRow(props: {
  attachment: DatasetAttachment | ProjectAttachment;
  parentType: "dataset" | "project";
  parentId: number;
  parentName: string;
}) {
  const { attachment, parentType, parentId, parentName } = props;
  const [open, setOpen] = useState(false);
  const { has_permission } = useAuth();

  const can_modify = has_permission("projects", "modify");

  const handleDownload = () => {
    window.location.href = `${server_address}/api/v1/external_files/${attachment.id}/download`;
  };

  return (
    <React.Fragment>
      <TableRow sx={{ "& > *": { borderBottom: "unset" } }}>
        <TableCell width="50px">
          <IconButton
            aria-label="expand row"
            size="small"
            onClick={() => setOpen(!open)}
          >
            {open ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell component="th" scope="row" sx={{ fontWeight: "bold" }}>
          {attachment.file_name}
        </TableCell>
        <TableCell>
          <AttachmentTypeChip type={attachment.attachment_type} />
        </TableCell>
        <TableCell>
          <AttachmentStatusChip status={attachment.status} />
        </TableCell>
        <TableCell>{dateStringToLocalTime(attachment.created_on)}</TableCell>
        <TableCell>{formatSize(attachment.file_size)}</TableCell>
        <TableCell align="right">
          <Box sx={{ display: "flex", justifyContent: "flex-end" }}>
            <Tooltip title="Download">
              <IconButton size="small" onClick={handleDownload}>
                <DownloadIcon fontSize="small" />
              </IconButton>
            </Tooltip>
            <OpenAttachmentButton attachment={attachment} />
            <DeleteAttachmentButton
              attachment={attachment}
              parentType={parentType}
              parentId={parentId}
              parentName={parentName}
              disabled={!can_modify}
            />
          </Box>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={8}>
          <Collapse in={open} timeout="auto" unmountOnExit>
            <Box sx={{ margin: 2 }}>
              <Typography variant="h6" gutterBottom component="div">
                Description
              </Typography>
              <Typography variant="body1" sx={{ whiteSpace: "pre-wrap" }}>
                {attachment.description || (
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    component="span"
                    sx={{ fontStyle: "italic" }}
                  >
                    No description provided.
                  </Typography>
                )}
              </Typography>

              <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
                SHA-256 SUM
              </Typography>
              <Typography
                variant={"body2"}
                fontFamily={"monospace"}
                sx={{ whiteSpace: "pre-wrap" }}
              >
                {attachment.sha256sum}
              </Typography>

              {attachment.provenance &&
                Object.keys(attachment.provenance).length > 0 && (
                  <>
                    <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
                      Provenance
                    </Typography>
                    <GenericDataList
                      data={attachment.provenance}
                      keys={Object.keys(attachment.provenance)}
                    />
                  </>
                )}
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
}

export const AttachmentTable: React.FC<AttachmentTableProps> = ({
  attachments,
  parentType,
  parentId,
  parentName,
}) => {
  if (attachments.length === 0) {
    return (
      <Typography variant="body1" color="text.secondary" sx={{ p: 2 }}>
        No attachments found.
      </Typography>
    );
  }

  return (
    <>
      <TableContainer component={Paper} variant="outlined">
        <Table aria-label="attachments table" size="small">
          <TableHead>
            <TableRow>
              <TableCell />
              <TableCell>Filename</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Created On</TableCell>
              <TableCell>Size</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {attachments.map((attachment) => (
              <AttachmentRow
                key={attachment.id}
                attachment={attachment}
                parentType={parentType}
                parentId={parentId}
                parentName={parentName}
              />
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
};

export default AttachmentTable;
