import React, { useEffect, useState } from "react";
import {
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Button,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import RemoveIcon from "@mui/icons-material/Remove";
import AddIcon from "@mui/icons-material/Add";
import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import { Box, IconButton, Chip, Divider } from "@mui/material";
import * as qcpTypes from "../PortalTypes";

interface ComputeHistoryProps {
  recordType: string;
  recordId: number;
}

const ComputeHistory: React.FC<ComputeHistoryProps> = ({
  recordType,
  recordId,
}) => {
  const [computeHistoryFetchedData, setComputeHistoryFetchedData] =
    React.useState<FetchedData<qcpTypes.ComputeHistory>>({
      data: undefined,
      error: undefined,
      loading: true,
    });
  const [isExpanded, setIsExpanded] = useState(false); 
  const { fetchData } = usePortalClientRequest();

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

  useEffect(() => {
    fetchData<qcpTypes.ComputeHistory>(
      setComputeHistoryFetchedData,
      "get",
      `/api/v1/records/${recordType}/${recordId}?include=compute_history`
    );
  }, [fetchData, recordType, recordId]);

  const computeHistory = computeHistoryFetchedData?.data?.compute_history || [];
  return (
    <>
      {/* Title with toggle button */}
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
      <Divider sx={{ my: 1 }} />

      {/* Content */}
      {isExpanded && (
        <>
          {computeHistoryFetchedData.loading && (
            <Typography>Loading...</Typography>
          )}
          {computeHistoryFetchedData.error && (
            <Typography color="error">
              Error: {computeHistoryFetchedData.error}
            </Typography>
          )}
          {!computeHistoryFetchedData.loading &&
            computeHistory.length === 0 && (
              <Typography>
                There is no compute history for this record.
              </Typography>
            )}
          {!computeHistoryFetchedData.loading && computeHistory.length > 0 && (
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
                  {computeHistory.map((history, index) => (
                    <TableRow key={index}>
                      <TableCell>{history.id || "N/A"}</TableCell>
                      <TableCell>{history.manager_name || "N/A"}</TableCell>
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
                            console.log(`View output for ID: ${history.id}`);
                          }}
                        >
                          View Output
                        </Button>
                      </TableCell>
                      <TableCell>
                        {history.provenance ? (
                          <Box component="ul" sx={{ pl: 2, m: 0 }}>
                            {Object.entries(history.provenance).map(
                              ([key, value]) => (
                                <li key={key}>
                                  <Typography variant="body2">
                                    <strong>{key}:</strong> {value || "N/A"}
                                  </Typography>
                                </li>
                              )
                            )}
                          </Box>
                        ) : (
                          <Typography variant="body2">None</Typography>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </>
      )}
    </>
  );
};

export default ComputeHistory;
