import React from "react";
import {
  Link,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@mui/material";
import * as qcpTypes from "../../PortalTypes";
import { calculateTotalStatusCounts } from "../../Utils.ts";

interface DatasetStatusTableProps {
  statusData: qcpTypes.DatasetStatus;
  onSelectStatus: (
    specificationName: string,
    status: qcpTypes.RecordStatus,
  ) => void;
}

function StatusCountLink({
  count,
  specificationName,
  status,
  onSelectStatus,
  strong = false,
}: {
  count: number;
  specificationName: string;
  status: qcpTypes.RecordStatus;
  onSelectStatus: (
    specificationName: string,
    status: qcpTypes.RecordStatus,
  ) => void;
  strong?: boolean;
}) {
  if (count === 0) {
    return strong ? <strong>0</strong> : <>0</>;
  }

  return (
    <Link
      component="button"
      type="button"
      underline="hover"
      color="inherit"
      onClick={() => onSelectStatus(specificationName, status)}
      sx={{
        fontWeight: strong ? "bold" : "inherit",
        cursor: "pointer",
      }}
    >
      {count}
    </Link>
  );
}

export default function DatasetStatusTable({
  statusData,
  onSelectStatus,
}: DatasetStatusTableProps) {
  const totalCounts = React.useMemo(() => {
    return calculateTotalStatusCounts(statusData);
  }, [statusData]);

  return (
    <TableContainer component={Paper} variant="outlined">
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>
              Specification
            </TableCell>
            <TableCell align="right">
              Complete
            </TableCell>
            <TableCell align="right">
              Waiting
            </TableCell>
            <TableCell align="right">
              Running
            </TableCell>
            <TableCell align="right">
              Error
            </TableCell>
            <TableCell align="right">
              Cancelled/Deleted/Invalid
            </TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {Object.entries(statusData).map(([specName, counts]) => (
            <TableRow key={specName}>
              <TableCell>{specName}</TableCell>
              <TableCell align="right">
                <StatusCountLink
                  count={counts.complete || 0}
                  specificationName={specName}
                  status="complete"
                  onSelectStatus={onSelectStatus}
                />
              </TableCell>
              <TableCell align="right">
                <StatusCountLink
                  count={counts.waiting || 0}
                  specificationName={specName}
                  status="waiting"
                  onSelectStatus={onSelectStatus}
                />
              </TableCell>
              <TableCell align="right">
                <StatusCountLink
                  count={counts.running || 0}
                  specificationName={specName}
                  status="running"
                  onSelectStatus={onSelectStatus}
                />
              </TableCell>
              <TableCell align="right">
                <StatusCountLink
                  count={counts.error || 0}
                  specificationName={specName}
                  status="error"
                  onSelectStatus={onSelectStatus}
                />
              </TableCell>
              <TableCell align="right">
                <StatusCountLink
                  count={counts.cancelled || 0}
                  specificationName={specName}
                  status="cancelled"
                  onSelectStatus={onSelectStatus}
                />{" "}
                /{" "}
                <StatusCountLink
                  count={counts.deleted || 0}
                  specificationName={specName}
                  status="deleted"
                  onSelectStatus={onSelectStatus}
                />{" "}
                /{" "}
                <StatusCountLink
                  count={counts.invalid || 0}
                  specificationName={specName}
                  status="invalid"
                  onSelectStatus={onSelectStatus}
                />
              </TableCell>
            </TableRow>
          ))}
          <TableRow sx={{ backgroundColor: "rgba(0, 0, 0, 0.05)" }}>
            <TableCell>
              <strong>Total</strong>
            </TableCell>
            <TableCell align="right">
              <StatusCountLink
                count={totalCounts.complete || 0}
                specificationName="all"
                status="complete"
                onSelectStatus={onSelectStatus}
                strong
              />
            </TableCell>
            <TableCell align="right">
              <StatusCountLink
                count={totalCounts.waiting || 0}
                specificationName="all"
                status="waiting"
                onSelectStatus={onSelectStatus}
                strong
              />
            </TableCell>
            <TableCell align="right">
              <StatusCountLink
                count={totalCounts.running || 0}
                specificationName="all"
                status="running"
                onSelectStatus={onSelectStatus}
                strong
              />
            </TableCell>
            <TableCell align="right">
              <StatusCountLink
                count={totalCounts.error || 0}
                specificationName="all"
                status="error"
                onSelectStatus={onSelectStatus}
                strong
              />
            </TableCell>
            <TableCell align="right">
              <StatusCountLink
                count={totalCounts.cancelled || 0}
                specificationName="all"
                status="cancelled"
                onSelectStatus={onSelectStatus}
                strong
              />{" "}
              /{" "}
              <StatusCountLink
                count={totalCounts.deleted || 0}
                specificationName="all"
                status="deleted"
                onSelectStatus={onSelectStatus}
                strong
              />{" "}
              /{" "}
              <StatusCountLink
                count={totalCounts.invalid || 0}
                specificationName="all"
                status="invalid"
                onSelectStatus={onSelectStatus}
                strong
              />
            </TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  );
}
