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
  Typography,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  DialogContentText,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import DownloadIcon from "@mui/icons-material/Download";
import DeleteIcon from "@mui/icons-material/Delete";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import { DatasetAttachment, ProjectAttachment } from "../PortalTypes";
import AttachmentStatusChip from "./AttachmentStatusChip";
import AttachmentTypeChip from "./AttachmentTypeChip";
import { parseToDate } from "../Utils";
import { GenericDataList } from "./GenericDataList";
import { server_address } from "../request_config";
import { formatSize } from "../Utils";
import { useAuth } from "../Auth.tsx";
import { usePortalClient } from "../PortalClient.tsx";
import { useQueryClient } from "@tanstack/react-query";

interface AttachmentTableProps {
  attachments: (DatasetAttachment | ProjectAttachment)[];
  projectId?: string;
  datasetId?: string;
}

function AttachmentRow(props: {
  attachment: DatasetAttachment | ProjectAttachment;
  onDelete: (attachment: DatasetAttachment | ProjectAttachment) => void;
}) {
  const { attachment, onDelete } = props;
  const [open, setOpen] = useState(false);
  const { has_permission } = useAuth();

  const can_modify = has_permission("projects", "modify")

  const formattedDate =
    parseToDate(attachment.created_on)?.toLocaleString() ||
    attachment.created_on;


  const handleDownload = () => {
    const downloadUrl = `${server_address}/api/v1/external_files/${attachment.id}/download`;
    window.location.href = downloadUrl;
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
        <TableCell>{formattedDate}</TableCell>
        <TableCell>{formatSize(attachment.file_size)}</TableCell>
        <TableCell align="right">
          <Box sx={{ display: "flex", justifyContent: "flex-end" }}>
            <Tooltip title="Download">
              <IconButton size="small" onClick={handleDownload}>
                <DownloadIcon fontSize="small" />
              </IconButton>
            </Tooltip>
            <Tooltip title="Open">
              <IconButton size="small">
                <OpenInNewIcon fontSize="small" />
              </IconButton>
            </Tooltip>
            <Tooltip title="Delete">
              <IconButton disabled={!can_modify} size="small" onClick={() => onDelete(attachment)}>
                <DeleteIcon fontSize="small" />
              </IconButton>
            </Tooltip>
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
                SHA256SUM
              </Typography>
              <Typography variant={"body2"} fontFamily={"monospace"} sx={{ whiteSpace: "pre-wrap" }}>
                {attachment.sha256sum}
              </Typography>

              {attachment.provenance &&
                Object.keys(attachment.provenance).length > 0 && (
                  <>
                    <Typography
                      variant="h6"
                      gutterBottom
                      sx={{ mt: 2 }}
                    >
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
  projectId,
  datasetId,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [itemToDelete, setItemToDelete] = useState<DatasetAttachment | ProjectAttachment | null>(null);
  const [deleting, setDeleting] = useState(false);

  const handleDeleteClick = (attachment: DatasetAttachment | ProjectAttachment) => {
    setItemToDelete(attachment);
    setDeleteDialogOpen(true);
  };

  const handleConfirmDelete = async () => {
    if (!itemToDelete) return;
    setDeleting(true);
    try {
      if (projectId) {
        await makeRequest("DELETE", `api/v1/projects/${projectId}/attachments/${itemToDelete.id}`);
        queryClient.invalidateQueries({ queryKey: ["projectAttachments", projectId] });
      } else if (datasetId) {
        await makeRequest("DELETE", `api/v1/datasets/${datasetId}/attachments/${itemToDelete.id}`);
        queryClient.invalidateQueries({ queryKey: ["datasetAttachments", datasetId] });
      }
      setDeleteDialogOpen(false);
    } catch (error) {
      console.error("Failed to delete attachment:", error);
      // You might want to show an error message to the user here
    } finally {
      setDeleting(false);
      setItemToDelete(null);
    }
  };

  if (attachments.length === 0) {
    return (
      <Typography variant="body1" color="text.secondary" sx={{ p: 2 }}>
        No attachments found.
      </Typography>
    );
  }

  const formattedDate = itemToDelete ? (parseToDate(itemToDelete.created_on)?.toLocaleString() || itemToDelete.created_on) : "";

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
                onDelete={handleDeleteClick}
              />
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog
        open={deleteDialogOpen}
        onClose={() => !deleting && setDeleteDialogOpen(false)}
      >
        <DialogTitle>Confirm Delete</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to delete the following attachment?
          </DialogContentText>
          {itemToDelete && (
            <Box sx={{ mt: 2, p: 2, bgcolor: "action.hover", borderRadius: 1 }}>
              <Typography variant="body2">
                <strong>Filename:</strong> {itemToDelete.file_name}
              </Typography>
              <Typography variant="body2">
                <strong>Size:</strong> {formatSize(itemToDelete.file_size)}
              </Typography>
              <Typography variant="body2">
                <strong>Created On:</strong> {formattedDate}
              </Typography>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => setDeleteDialogOpen(false)}
            disabled={deleting}
          >
            Cancel
          </Button>
          <Button
            onClick={handleConfirmDelete}
            color="error"
            variant="contained"
            disabled={deleting}
          >
            {deleting ? "Deleting..." : "Delete"}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default AttachmentTable;
