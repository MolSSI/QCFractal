import React, { useState } from "react";
import {
  Box,
  Button,
  Chip,
  Dialog,
  DialogContent,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import RemoveIcon from "@mui/icons-material/Remove";
import AddIcon from "@mui/icons-material/Add";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import { ManagerFragment } from "./ManagerFragment";
import OutputFragment from "./OutputFragment";
import { useQuery } from "@tanstack/react-query";

interface ComputeHistoryProps {
  recordType: string;
  recordId: number;
}

const ComputeHistory: React.FC<ComputeHistoryProps> = ({
  recordType,
  recordId,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [managerDialogOpen, setManagerDialogOpen] = useState(false);
  const [selectedManager, setSelectedManager] = useState<string | null>(null);
  const [outputDialogOpen, setOutputDialogOpen] = useState(false);
  const [selectedComputeHistoryId, setSelectedComputeHistoryId] = useState<
    number | null
  >(null);
  const { makeRequest } = usePortalClient();

  const {
    status: computeHistoryStatus,
    data: computeHistoryData,
    error: computeHistoryError,
  } = useQuery({
    queryKey: ["recordComputeHistory", recordType, recordId],
    queryFn: () =>
      makeRequest<qcpTypes.ComputeHistory[]>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history`,
      ),
  });

  // Status color mapping (updated to match Record component)
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

  const computeHistory = computeHistoryData || [];
  return (
    <>
      {/* Title with toggle button and buttons */}
      <Box
        display="flex"
        alignItems="center"
        justifyContent="space-between"
        mb={2}
      >
        <Box display="flex" alignItems="center" gap={1}>
          <IconButton
            size="small"
            onClick={() => setIsExpanded(!isExpanded)}
            aria-label="toggle compute history"
          >
            {isExpanded ? <RemoveIcon /> : <AddIcon />}
          </IconButton>
          <Typography variant="h6" fontWeight="bold">
            Compute History
          </Typography>
        </Box>
        <Box display="flex" gap={1}>
          <Button
            variant="outlined"
            size="small"
            disabled={
              computeHistoryStatus === "pending" || computeHistory.length === 0
            }
            onClick={() => {
              if (computeHistory.length > 0) {
                setSelectedComputeHistoryId(
                  computeHistory[computeHistory.length - 1].id,
                ); // Use the first item's ID
                setOutputDialogOpen(true); // Open the Output dialog
              }
            }}
          >
            View Outputs
          </Button>
          <Button
            variant="outlined"
            size="small"
            onClick={() => {
              console.log("Native Files clicked");
            }}
          >
            Native Files
          </Button>
          <Button
            variant="outlined"
            size="small"
            onClick={() => {
              console.log("Wave Functions clicked");
            }}
          >
            Wave Functions
          </Button>
        </Box>
      </Box>

      {/* Content */}
      {isExpanded && (
        <>
          {computeHistoryStatus === "pending" && <Typography>Loading...</Typography>}
          {computeHistoryStatus === "error" && (
            <Typography color="error">
              Error: {computeHistoryError.message}
            </Typography>
          )}
          {computeHistoryStatus === "success" &&
            computeHistory.length === 0 && (
              <Typography>
                There is no compute history for this record.
              </Typography>
            )}
          {computeHistoryStatus === "success" && computeHistory.length > 0 && (
            <TableContainer component={Paper} sx={{ boxShadow: "none" }}>
              <Table
                size="small"
                aria-label="compute history table"
                sx={{
                  borderCollapse: "collapse",
                  "& td, & th": { borderBottom: "1px solid #ccc" }, // Horizontal lines only
                }}
              >
                <TableHead>
                  <TableRow>
                    <TableCell>
                      <strong>ID</strong>
                    </TableCell>
                    <TableCell>
                      <strong>Manager Name</strong>
                    </TableCell>
                    <TableCell>
                      <strong>Date</strong>
                    </TableCell>
                    <TableCell>
                      <strong>Status</strong>
                    </TableCell>
                    <TableCell>
                      <strong>Output</strong>
                    </TableCell>
                    <TableCell>
                      <strong>Provenance</strong>
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {computeHistory.map(
                    (history: qcpTypes.ComputeHistory, index) => (
                      <TableRow key={index}>
                        <TableCell>{history.id || "N/A"}</TableCell>
                        <TableCell>
                          <Typography
                            variant="body2"
                            color="primary"
                            sx={{
                              cursor: "pointer",
                              textDecoration: "underline",
                            }}
                            onClick={() => {
                              setSelectedManager(history.manager_name);
                              setManagerDialogOpen(true);
                            }}
                          >
                            {history.manager_name || "N/A"}
                          </Typography>
                        </TableCell>
                        <TableCell>
                          {new Date(history.modified_on).toLocaleString() ||
                            "N/A"}
                        </TableCell>
                        <TableCell>
                          <Chip
                            label={history.status || "N/A"}
                            color={
                              statusColors[history.status.toLowerCase()] ||
                              "default"
                            }
                            sx={{
                              fontWeight: "bold",
                              textTransform: "capitalize",
                            }}
                          />
                        </TableCell>
                        <TableCell>
                          <Button
                            variant="outlined"
                            size="small"
                            onClick={() => {
                              setSelectedComputeHistoryId(history.id);
                              setOutputDialogOpen(true);
                            }}
                          >
                            View Output
                          </Button>
                        </TableCell>
                        <TableCell>
                          {history.provenance ? (
                            <Box
                              component="ul"
                              sx={{
                                pl: 2,
                                m: 0,
                                listStyleType: "none", // Remove bullet points
                              }}
                            >
                              {Object.entries(history.provenance).map(
                                ([key, value]) => (
                                  <li key={key}>
                                    <Typography variant="body2">
                                      <strong>{key}:</strong> {value || "N/A"}
                                    </Typography>
                                  </li>
                                ),
                              )}
                            </Box>
                          ) : (
                            <Typography variant="body2">None</Typography>
                          )}
                        </TableCell>
                      </TableRow>
                    ),
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </>
      )}
      {/* Manager Dialog */}
      <Dialog
        fullWidth={true}
        open={managerDialogOpen}
        onClose={() => setManagerDialogOpen(false)}
      >
        <DialogContent>
          {selectedManager && <ManagerFragment managerName={selectedManager} />}
        </DialogContent>
      </Dialog>

      {/* Output Dialog */}
      <Dialog
        fullWidth={true}
        maxWidth="md"
        open={outputDialogOpen}
        onClose={() => setOutputDialogOpen(false)}
      >
        <DialogContent>
          {selectedComputeHistoryId && (
            <OutputFragment
              recordType={recordType}
              recordId={recordId}
              computeHistoryId={selectedComputeHistoryId}
            />
          )}
        </DialogContent>
      </Dialog>
    </>
  );
};

export default ComputeHistory;
