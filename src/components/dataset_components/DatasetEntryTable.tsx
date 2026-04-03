import React from "react";
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
  TablePagination,
  TableRow,
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import LoadingIndicator from "../LoadingIndicator";
import { usePortalClient } from "../../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import * as qcpTypes from "../../PortalTypes";
import { getDatasetEntryComponent } from "../record_components/lookup.tsx";

function EntryRow({
  entryName,
  datasetType,
  datasetId,
}: {
  entryName: string;
  datasetType: qcpTypes.RecordType;
  datasetId: string;
}) {
  const [expanded, setExpanded] = React.useState(false);
  const { makeRequest } = usePortalClient();

  const { data: entryData, status } = useQuery({
    queryKey: ["datasetEntry", datasetType, datasetId, entryName],
    queryFn: () =>
      makeRequest<Record<string, unknown>>(
        "POST",
        `api/v1/datasets/${datasetType}/${datasetId}/entries/bulkFetch`,
        { names: [entryName] },
      ),
    enabled: expanded,
  });

  return (
    <React.Fragment>
      <TableRow
        hover
        sx={{ cursor: "pointer" }}
        onClick={() => setExpanded(!expanded)}
      >
        <TableCell width="50px">
          <IconButton size="small">
            {expanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell>
          <Typography variant="body2" fontWeight="medium">
            {entryName}
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={2}>
          <Collapse in={expanded} timeout="auto" unmountOnExit>
            <Box sx={{ p: 2 }}>
              {status === "pending" && <LoadingIndicator />}
              {status === "error" && (
                <Typography color="error">Error fetching entry data</Typography>
              )}
              {status === "success" &&
                entryData &&
                (() => {
                  const EntryComponent = getDatasetEntryComponent(datasetType);
                  return <EntryComponent entry={entryData[entryName]} />;
                })()}
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
}

interface DatasetEntryTableProps {
  entryNames: string[];
  datasetType: qcpTypes.RecordType;
  datasetId: string;
}

export default function DatasetEntryTable({
  entryNames,
  datasetType,
  datasetId,
}: DatasetEntryTableProps) {
  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(10);

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  return (
    <>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell width="50px" />
              <TableCell>
                <strong>Name</strong>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {entryNames
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((entryName) => (
                <EntryRow
                  key={entryName}
                  entryName={entryName}
                  datasetType={datasetType}
                  datasetId={datasetId}
                />
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 25, 50]}
        component="div"
        count={entryNames.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </>
  );
}
