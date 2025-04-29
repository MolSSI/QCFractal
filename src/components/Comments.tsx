import React from "react";
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
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

export default function Comments() {
  return (
    <Accordion>
      {/* Accordion Header */}
      <AccordionSummary
        expandIcon={<ExpandMoreIcon />}
        aria-controls="comments-content"
        id="comments-header"
      >
        <Typography variant="h6" fontWeight="bold">
          Comments
        </Typography>
      </AccordionSummary>

      {/* Accordion Content */}
      <AccordionDetails>
        <TableContainer component={Paper}>
          <Table size="small" aria-label="comments table">
            <TableHead>
              <TableRow>
                <TableCell>
                  <strong>Time/Date</strong>
                </TableCell>
                <TableCell>
                  <strong>User</strong>
                </TableCell>
                <TableCell>
                  <strong>Comment</strong>
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {/* Placeholder Rows */}
              <TableRow>
                <TableCell>2025-04-29 10:00</TableCell>
                <TableCell>John Doe</TableCell>
                <TableCell>This is a placeholder comment.</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>2025-04-28 15:30</TableCell>
                <TableCell>Jane Smith</TableCell>
                <TableCell>Another placeholder comment.</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </TableContainer>
      </AccordionDetails>
    </Accordion>
  );
}
