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
import CheckIcon from "@mui/icons-material/Check";
import CloseIcon from "@mui/icons-material/Close";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import * as qcpTypes from "../../PortalTypes";
import { useAuth } from "../../Auth.tsx";
import { describeRequestError } from "../../Utils.ts";
import { getSpecificationComponent } from "../record_components/lookup.tsx";
import {
  DeleteSpecificationDialog,
  useModifySpecificationDescription,
  useRenameSpecification,
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

  const [editingSpecName, setEditingSpecName] = React.useState<string | null>(
    null,
  );
  const [editingValue, setEditingValue] = React.useState("");
  const [specToDelete, setSpecToDelete] = React.useState<string | null>(null);

  const renameMutation = useRenameSpecification(datasetType, datasetId);

  const [editingDescSpecName, setEditingDescSpecName] = React.useState<
    string | null
  >(null);
  const [editingDescValue, setEditingDescValue] = React.useState("");
  const descriptionMutation = useModifySpecificationDescription(
    datasetType,
    datasetId,
  );

  const startEditingDescription = (specName: string) => {
    descriptionMutation.reset();
    setEditingDescSpecName(specName);
    setEditingDescValue(specificationsData[specName].description ?? "");
  };

  const cancelEditingDescription = () => {
    descriptionMutation.reset();
    setEditingDescSpecName(null);
    setEditingDescValue("");
  };

  const saveDescription = () => {
    if (!editingDescSpecName || descriptionMutation.isPending) {
      return;
    }

    descriptionMutation.mutate(
      { name: editingDescSpecName, description: editingDescValue },
      { onSuccess: cancelEditingDescription },
    );
  };

  const startEditing = (specName: string) => {
    renameMutation.reset();
    setEditingSpecName(specName);
    setEditingValue(specName);
  };

  const cancelEditing = () => {
    renameMutation.reset();
    setEditingSpecName(null);
    setEditingValue("");
  };

  const trimmedEditingValue = editingValue.trim();
  const isDuplicateName =
    trimmedEditingValue !== editingSpecName &&
    Object.prototype.hasOwnProperty.call(
      specificationsData,
      trimmedEditingValue,
    );
  const canSaveRename =
    trimmedEditingValue !== "" &&
    trimmedEditingValue !== editingSpecName &&
    !isDuplicateName &&
    !renameMutation.isPending;

  const saveRename = () => {
    if (!editingSpecName || !canSaveRename) {
      return;
    }

    renameMutation.mutate(
      { oldName: editingSpecName, newName: trimmedEditingValue },
      {
        onSuccess: () => {
          // Keep the row expanded under its new name if it was open
          setExpandedSpecName((prev) =>
            prev === editingSpecName ? trimmedEditingValue : prev,
          );
          setEditingSpecName(null);
          setEditingValue("");
        },
      },
    );
  };

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
                      {editingSpecName === specName ? (
                        // Clicks inside the editor must not toggle the row
                        <Box
                          sx={{
                            display: "flex",
                            alignItems: "center",
                            gap: 0.5,
                          }}
                          onClick={(event) => event.stopPropagation()}
                        >
                          <TextField
                            size="small"
                            autoFocus
                            value={editingValue}
                            onChange={(e) => setEditingValue(e.target.value)}
                            onKeyDown={(event) => {
                              if (event.key === "Enter") {
                                event.preventDefault();
                                saveRename();
                              } else if (event.key === "Escape") {
                                event.preventDefault();
                                cancelEditing();
                              }
                            }}
                            error={isDuplicateName || renameMutation.isError}
                            helperText={
                              isDuplicateName
                                ? "A specification with that name already exists"
                                : renameMutation.isError
                                  ? describeRequestError(
                                      renameMutation.error,
                                      "Failed to rename specification",
                                    )
                                  : undefined
                            }
                            // Float the helper text so it doesn't offset the
                            // input from the adjacent buttons
                            sx={{
                              position: "relative",
                              "& .MuiFormHelperText-root": {
                                position: "absolute",
                                top: "100%",
                                left: 0,
                                mt: 0.25,
                                mx: 0,
                                whiteSpace: "nowrap",
                              },
                            }}
                          />
                          <Tooltip title="Save">
                            <span>
                              <IconButton
                                size="small"
                                color="primary"
                                disabled={!canSaveRename}
                                onClick={saveRename}
                              >
                                <CheckIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                          <Tooltip title="Cancel">
                            <span>
                              <IconButton
                                size="small"
                                disabled={renameMutation.isPending}
                                onClick={cancelEditing}
                              >
                                <CloseIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                        </Box>
                      ) : (
                        <Box
                          sx={{
                            display: "flex",
                            alignItems: "center",
                            gap: 0.5,
                          }}
                        >
                          <Typography variant="body2" fontWeight="medium">
                            {specName}
                          </Typography>
                          {canModify && (
                            <Tooltip title="Rename specification">
                              <IconButton
                                size="small"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  startEditing(specName);
                                }}
                              >
                                <EditIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
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
                        </Box>
                      )}
                    </TableCell>
                    <TableCell sx={{ maxWidth: 0, width: "60%" }}>
                      {editingDescSpecName === specName ? (
                        // Clicks inside the editor must not toggle the row
                        <Box
                          sx={{
                            display: "flex",
                            alignItems: "flex-start",
                            gap: 0.5,
                          }}
                          onClick={(event) => event.stopPropagation()}
                        >
                          <TextField
                            size="small"
                            autoFocus
                            fullWidth
                            multiline
                            maxRows={8}
                            value={editingDescValue}
                            onChange={(e) =>
                              setEditingDescValue(e.target.value)
                            }
                            onKeyDown={(event) => {
                              // Enter inserts a newline; Ctrl/Cmd+Enter saves
                              if (
                                event.key === "Enter" &&
                                (event.ctrlKey || event.metaKey)
                              ) {
                                event.preventDefault();
                                saveDescription();
                              } else if (event.key === "Escape") {
                                event.preventDefault();
                                cancelEditingDescription();
                              }
                            }}
                            error={descriptionMutation.isError}
                            helperText={
                              descriptionMutation.isError
                                ? describeRequestError(
                                    descriptionMutation.error,
                                    "Failed to update description",
                                  )
                                : undefined
                            }
                          />
                          <Tooltip title="Save">
                            <span>
                              <IconButton
                                size="small"
                                color="primary"
                                disabled={descriptionMutation.isPending}
                                onClick={saveDescription}
                              >
                                <CheckIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                          <Tooltip title="Cancel">
                            <span>
                              <IconButton
                                size="small"
                                disabled={descriptionMutation.isPending}
                                onClick={cancelEditingDescription}
                              >
                                <CloseIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                        </Box>
                      ) : (
                        <Box sx={{ display: "flex", alignItems: "center" }}>
                          {/* Collapsed rows show only the first line; the full
                              text is in the expanded section below */}
                          <Typography
                            variant="body2"
                            color="text.secondary"
                            noWrap
                          >
                            {firstLine(
                              specificationsData[specName].description,
                            )}
                          </Typography>
                          {canModify && (
                            <Tooltip title="Edit description">
                              <IconButton
                                size="small"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  startEditingDescription(specName);
                                }}
                              >
                                <EditIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                        </Box>
                      )}
                    </TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell
                      style={{ paddingBottom: 0, paddingTop: 0 }}
                      colSpan={3}
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
