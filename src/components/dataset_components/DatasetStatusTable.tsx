import React from "react";
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@mui/material";
import * as qcpTypes from "../../PortalTypes";

interface DatasetStatusTableProps {
  statusData: qcpTypes.DatasetStatus;
}

export default function DatasetStatusTable({ statusData }: DatasetStatusTableProps) {
  const totalCounts = Object.values(statusData).reduce(
    (acc, counts) => {
      Object.entries(counts).forEach(([status, count]) => {
        const s = status as qcpTypes.RecordStatus;
        acc[s] = (acc[s] || 0) + count;
      });
      return acc;
    },
    {} as Record<qcpTypes.RecordStatus, number>,
  );

  return (
    <TableContainer component={Paper} variant="outlined">
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>
              <strong>Specification</strong>
            </TableCell>
            <TableCell align="right">
              <strong>Complete</strong>
            </TableCell>
            <TableCell align="right">
              <strong>Waiting</strong>
            </TableCell>
            <TableCell align="right">
              <strong>Running</strong>
            </TableCell>
            <TableCell align="right">
              <strong>Error</strong>
            </TableCell>
            <TableCell align="right">
              <strong>Cancelled/Deleted/Invalid</strong>
            </TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {Object.entries(statusData).map(([specName, counts]) => (
            <TableRow key={specName}>
              <TableCell>{specName}</TableCell>
              <TableCell align="right">{counts.complete || 0}</TableCell>
              <TableCell align="right">{counts.waiting || 0}</TableCell>
              <TableCell align="right">{counts.running || 0}</TableCell>
              <TableCell align="right">{counts.error || 0}</TableCell>
              <TableCell align="right">
                {counts.cancelled || 0} / {counts.deleted || 0} /{" "}
                {counts.invalid || 0}
              </TableCell>
            </TableRow>
          ))}
          <TableRow sx={{ backgroundColor: "rgba(0, 0, 0, 0.05)" }}>
            <TableCell>
              <strong>Total</strong>
            </TableCell>
            <TableCell align="right">
              <strong>{totalCounts.complete || 0}</strong>
            </TableCell>
            <TableCell align="right">
              <strong>{totalCounts.waiting || 0}</strong>
            </TableCell>
            <TableCell align="right">
              <strong>{totalCounts.running || 0}</strong>
            </TableCell>
            <TableCell align="right">
              <strong>{totalCounts.error || 0}</strong>
            </TableCell>
            <TableCell align="right">
              <strong>
                {totalCounts.cancelled || 0} / {totalCounts.deleted || 0} /{" "}
                {totalCounts.invalid || 0}
              </strong>
            </TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  );
}
