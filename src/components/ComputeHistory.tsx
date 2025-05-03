import React, { useEffect } from "react";
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
import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import { Chip } from "@mui/material";
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
      {computeHistoryFetchedData.loading && <Typography>Loading...</Typography>}
      {computeHistoryFetchedData.error && (
        <Typography color="error">
          Error: {computeHistoryFetchedData.error}
        </Typography>
      )}
      {!computeHistoryFetchedData.loading && computeHistory && (
        <>
          <Accordion>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="compute-history-content"
              id="compute-history-header"
            >
              <Typography variant="h6" fontWeight="bold">
                Compute History
              </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <TableContainer component={Paper}>
                <Table size="small" aria-label="compute history table">
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
                              // Placeholder for output dialog logic
                              console.log(`View output for ID: ${history.id}`);
                            }}
                          >
                            View Output
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </AccordionDetails>
          </Accordion>
        </>
      )}
    </>
  );
};

export default ComputeHistory;
