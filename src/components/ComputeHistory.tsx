import React, { useState } from "react";
import {
  Box,
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
import * as qcpTypes from "../PortalTypes";
import ManagerLink from "./ManagerLink";
import ViewOutput from "./ViewOutput.tsx";
import Status from "./Status.tsx";

interface ComputeHistoryProps {
  recordData: qcpTypes.RecordData;
}

const ComputeHistory: React.FC<ComputeHistoryProps> = ({ recordData }) => {
  const [isExpanded, setIsExpanded] = useState(false);

  const recordId = recordData.id;
  const recordType = recordData.record_type;

  const computeHistory = recordData.compute_history || [];

  return (
    <>
      {/* Title with toggle button and buttons */}
      <Box display="flex" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center" gap={1} flex="1">
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
        <Box flex="1">
          {/* Last Manager Section */}
          <Box p={0} sx={{ height: "100%" }}>
            <Typography variant="body1" fontWeight="bold">
              Last Manager:
            </Typography>
            {recordData.manager_name ? (
              <ManagerLink managerName={recordData.manager_name} />
            ) : (
              <Typography variant="body2" color="text.secondary">
                (none)
              </Typography>
            )}
          </Box>
        </Box>
      </Box>

      {/* Content */}
      {isExpanded && (
        <>
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
                ID
              </TableCell>
              <TableCell>
                Manager Name
              </TableCell>
              <TableCell>
                Date
              </TableCell>
              <TableCell>
                Status
              </TableCell>
              <TableCell>
                Output
              </TableCell>
              <TableCell>
                Provenance
              </TableCell>
            </TableRow>
          </TableHead>
              <TableBody>
                {computeHistory.map(
                  (history: qcpTypes.ComputeHistory, index) => (
                    <TableRow key={index}>
                      <TableCell>{history.id || "N/A"}</TableCell>
                      <TableCell>
                        {history.manager_name ? (
                          <ManagerLink managerName={history.manager_name} />
                        ) : (
                          <Typography variant="body2">N/A</Typography>
                        )}
                      </TableCell>
                      <TableCell>
                        {new Date(history.modified_on).toLocaleString() ||
                          "N/A"}
                      </TableCell>
                      <TableCell>
                        <Status status={history.status} />
                      </TableCell>
                      <TableCell>
                        <ViewOutput
                          recordType={recordType}
                          recordId={recordId}
                          computeHistoryId={history.id}
                        />
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
        </>
      )}
    </>
  );
};

export default ComputeHistory;
