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
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import * as qcpTypes from "../../PortalTypes";
import { useAuth } from "../../Auth.tsx";
import { getSpecificationComponent } from "../record_components/lookup.tsx";
import {
  DeleteSpecificationDialog,
  EditSpecificationDialog,
} from "./DatasetSpecificationActions.tsx";

// Every record in a dataset carries a status, so summing the per-status counts
// for one specification gives the number of records attached to it.
function countRecordsForSpec(
  datasetStatus: qcpTypes.DatasetStatus | undefined,
  specName: string,
): number | undefined {
  if (!datasetStatus) {
    return undefined;
  }

  return Object.values(datasetStatus[specName] ?? {}).reduce(
    (total, count) => total + count,
    0,
  );
}

function firstLine(text: string | undefined): string {
  return (text ?? "").trim().split(/\r?\n/, 1)[0];
}

interface DatasetSpecificationTableProps {
  specificationsData: Record<
    string,
    { specification: qcpTypes.DatasetSpecificationData; description?: string }
  >;
  datasetType: qcpTypes.RecordType;
  datasetId: number;
  datasetStatus: qcpTypes.DatasetStatus | undefined;
}

export default function DatasetSpecificationTable({
  specificationsData,
  datasetType,
  datasetId,
  datasetStatus,
}: DatasetSpecificationTableProps) {
  const { has_permission, loggedIn } = useAuth();
  // The server checks datasets:modify for renaming AND for deleting a
  // specification, so both buttons hang off the same permission
  const canModify = loggedIn && has_permission("datasets", "modify");
  const canDelete = canModify && has_permission("datasets", "delete");
  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(10);
  const [filter, setFilter] = React.useState("");
  const [expandedSpecName, setExpandedSpecName] = React.useState<string | null>(
    null,
  );

  const specNames = React.useMemo(() => {
    return Object.keys(specificationsData).filter((name) =>
      name.toLowerCase().includes(filter.toLowerCase()),
    );
  }, [specificationsData, filter]);

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleFilterChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setFilter(event.target.value);
    setPage(0);
  };

  const toggleExpandSpec = (name: string) => {
    setExpandedSpecName((prev) => (prev === name ? null : name));
  };

  const [specToEdit, setSpecToEdit] = React.useState<string | null>(null);
  const [specToDelete, setSpecToDelete] = React.useState<string | null>(null);

  const SpecificationComponent = React.useMemo(
    () => getSpecificationComponent(datasetType),
    [datasetType],
  );

  return (
    <>
      <Box sx={{ mb: 2 }} width={"30%"}>
        <TextField
          fullWidth
          variant="outlined"
          size="small"
          label="Filter specifications"
          value={filter}
          onChange={handleFilterChange}
        />
      </Box>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell width="50px" />
              <TableCell>Name</TableCell>
              <TableCell>Description</TableCell>
              {canModify && <TableCell align="right">Actions</TableCell>}
            </TableRow>
          </TableHead>
          <TableBody>
            {specNames
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((specName) => (
                <React.Fragment key={specName}>
                  <TableRow
                    hover
                    sx={{ cursor: "pointer" }}
                    onClick={() => toggleExpandSpec(specName)}
                  >
                    <TableCell>
                      <IconButton size="small">
                        {expandedSpecName === specName ? (
                          <KeyboardArrowUpIcon />
                        ) : (
                          <KeyboardArrowDownIcon />
                        )}
                      </IconButton>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" fontWeight="medium">
                        {specName}
                      </Typography>
                    </TableCell>
                    <TableCell sx={{ maxWidth: 0, width: "60%" }}>
                      {/* Collapsed rows show only the first line; the full
                          text is in the expanded section below */}
                      <Typography variant="body2" color="text.secondary" noWrap>
                        {firstLine(specificationsData[specName].description)}
                      </Typography>
                    </TableCell>
                    {canModify && (
                      <TableCell align="right" sx={{ whiteSpace: "nowrap" }}>
                        <Tooltip title="Edit specification">
                          <IconButton
                            size="small"
                            onClick={(event) => {
                              event.stopPropagation();
                              setSpecToEdit(specName);
                            }}
                          >
                            <EditIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                        {canDelete && (
                          <Tooltip title="Delete specification">
                            <IconButton
                              size="small"
                              onClick={(event) => {
                                event.stopPropagation();
                                setSpecToDelete(specName);
                              }}
                            >
                              <DeleteIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        )}
                      </TableCell>
                    )}
                  </TableRow>
                  <TableRow>
                    <TableCell
                      style={{ paddingBottom: 0, paddingTop: 0 }}
                      colSpan={canModify ? 4 : 3}
                    >
                      <Collapse
                        in={expandedSpecName === specName}
                        timeout="auto"
                        unmountOnExit
                      >
                        <Box>
                          {specificationsData[specName].description?.trim() && (
                            <Typography
                              variant="body2"
                              sx={{ whiteSpace: "pre-wrap", py: 1 }}
                            >
                              {specificationsData[specName].description}
                            </Typography>
                          )}
                          <SpecificationComponent
                            specification={
                              specificationsData[specName].specification
                            }
                          />
                        </Box>
                      </Collapse>
                    </TableCell>
                  </TableRow>
                </React.Fragment>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 25, 50]}
        component="div"
        count={specNames.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
      {specToEdit !== null && specificationsData[specToEdit] && (
        <EditSpecificationDialog
          datasetType={datasetType}
          datasetId={datasetId}
          specName={specToEdit}
          description={specificationsData[specToEdit].description ?? ""}
          existingNames={Object.keys(specificationsData)}
          onRenamed={(oldName, newName) =>
            // Keep the row expanded under its new name if it was open
            setExpandedSpecName((prev) => (prev === oldName ? newName : prev))
          }
          onClose={() => setSpecToEdit(null)}
        />
      )}
      {specToDelete !== null && (
        <DeleteSpecificationDialog
          datasetType={datasetType}
          datasetId={datasetId}
          specName={specToDelete}
          recordCount={countRecordsForSpec(datasetStatus, specToDelete)}
          open
          onClose={() => setSpecToDelete(null)}
        />
      )}
    </>
  );
}
