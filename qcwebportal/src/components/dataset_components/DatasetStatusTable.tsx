import React from "react";
import {
  Box,
  Link,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
} from "@mui/material";
import * as qcpTypes from "../../PortalTypes";
import { calculateTotalStatusCounts } from "../../Utils.ts";

const ROWS_PER_PAGE = 10;

interface DatasetStatusTableProps {
  statusData: qcpTypes.DatasetStatus;
  page: number;
  specFilter: string;
  onPageChange: (page: number) => void;
  onSpecFilterChange: (specFilter: string) => void;
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
  page,
  specFilter,
  onPageChange,
  onSpecFilterChange,
  onSelectStatus,
}: DatasetStatusTableProps) {
  // Always every specification, so the Total row stays a dataset-wide summary
  // and its links keep matching the "all specifications" records view
  const totalCounts = React.useMemo(() => {
    return calculateTotalStatusCounts(statusData);
  }, [statusData]);

  const allSpecCount = Object.keys(statusData).length;

  const filteredRows = React.useMemo(() => {
    const needle = specFilter.trim().toLowerCase();
    const rows = Object.entries(statusData);

    return needle === ""
      ? rows
      : rows.filter(([specName]) => specName.toLowerCase().includes(needle));
  }, [statusData, specFilter]);

  // Clamp rather than store: rows can drop away under us when the filter
  // changes or a refresh returns fewer specifications
  const pageCount = Math.max(1, Math.ceil(filteredRows.length / ROWS_PER_PAGE));
  const currentPage = Math.min(page, pageCount - 1);
  const isPaginated = filteredRows.length > ROWS_PER_PAGE;

  const visibleRows = React.useMemo(
    () =>
      filteredRows.slice(
        currentPage * ROWS_PER_PAGE,
        currentPage * ROWS_PER_PAGE + ROWS_PER_PAGE,
      ),
    [filteredRows, currentPage],
  );

  const showsEverySpec = visibleRows.length === allSpecCount;

  return (
    <TableContainer component={Paper} variant="outlined">
      <Box p={2} pb={1}>
        <TextField
          size="small"
          fullWidth
          label="Filter by specification name"
          value={specFilter}
          onChange={(e) => onSpecFilterChange(e.target.value)}
        />
      </Box>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Specification</TableCell>
            <TableCell align="right">Complete</TableCell>
            <TableCell align="right">Waiting</TableCell>
            <TableCell align="right">Running</TableCell>
            <TableCell align="right">Error</TableCell>
            <TableCell align="right">Cancelled/Deleted/Invalid</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {visibleRows.length === 0 && (
            <TableRow>
              <TableCell colSpan={6} align="center">
                No specifications match &ldquo;{specFilter.trim()}&rdquo;
              </TableCell>
            </TableRow>
          )}
          {visibleRows.map(([specName, counts]) => (
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
          <TableRow
            sx={{
              backgroundColor: "action.hover",
              "& td": {
                borderTop: 2,
                borderTopColor: "text.primary",
                fontWeight: "bold",
              },
            }}
          >
            <TableCell>
              <strong>
                {showsEverySpec
                  ? "Total"
                  : `Total (all ${allSpecCount} specifications)`}
              </strong>
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
      {isPaginated && (
        <TablePagination
          component="div"
          count={filteredRows.length}
          page={currentPage}
          onPageChange={(_event, newPage) => onPageChange(newPage)}
          rowsPerPage={ROWS_PER_PAGE}
          rowsPerPageOptions={[ROWS_PER_PAGE]}
          labelDisplayedRows={({ from, to, count }) =>
            `${from}-${to} of ${count} specifications`
          }
        />
      )}
    </TableContainer>
  );
}
