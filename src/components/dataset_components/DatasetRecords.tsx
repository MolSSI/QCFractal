import React from "react";
import {
  Box,
  FormControl,
  InputLabel,
  LinearProgress,
  Link as MuiLink,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import { useInfiniteQuery } from "@tanstack/react-query";
import { useAuth } from "../../Auth.tsx";
import { usePortalClient } from "../../PortalClient.tsx";
import * as qcpTypes from "../../PortalTypes";
import { calculateTotalStatusCounts } from "../../Utils.ts";
import LoadingIndicator from "../LoadingIndicator";
import StatusChip from "../StatusChip.tsx";

interface DatasetRecordsProps {
  datasetId: number;
  datasetType: qcpTypes.RecordType;
  datasetStatus: qcpTypes.DatasetStatus;
  specifications: string[];
  entryNames: string[];
  totalRecords?: number;
  page: number;
  rowsPerPage: number;
  entryFilter: string;
  specFilter: string;
  statusFilter: "all" | qcpTypes.RecordStatus;
  onPageChange: (page: number) => void;
  onRowsPerPageChange: (rowsPerPage: number) => void;
  onEntryFilterChange: (entryFilter: string) => void;
  onSpecFilterChange: (specFilter: string) => void;
  onStatusFilterChange: (statusFilter: "all" | qcpTypes.RecordStatus) => void;
}

type DiscoveredDatasetRecordRow = {
  entry_name: string;
  specification_name: string;
  record_id: number;
  status: qcpTypes.RecordStatus;
};

type DatasetRecordCursor = {
  specIndex: number;
  entryOffset: number;
  scannedCount: number;
};

type DatasetRecordDiscoveryPage = {
  rows: DiscoveredDatasetRecordRow[];
  nextCursor?: DatasetRecordCursor;
  exhausted: boolean;
  scannedCount: number;
};

const PREFETCH_PAGE_COUNT = 3;
const UNKNOWN_TOTAL_COUNT = -1;
const INITIAL_CURSOR: DatasetRecordCursor = {
  specIndex: 0,
  entryOffset: 0,
  scannedCount: 0,
};

export default function DatasetRecords({
  datasetId,
  datasetType,
  datasetStatus,
  specifications,
  entryNames,
  totalRecords,
  page,
  rowsPerPage,
  entryFilter,
  specFilter,
  statusFilter,
  onPageChange,
  onRowsPerPageChange,
  onEntryFilterChange,
  onSpecFilterChange,
  onStatusFilterChange,
}: DatasetRecordsProps) {
  const { makeRequest } = usePortalClient();
  const { serverInfo } = useAuth();

  const filteredEntries = React.useMemo(() => {
    if (!entryFilter) {
      return entryNames;
    }

    const loweredFilter = entryFilter.toLowerCase();
    return entryNames.filter((name) =>
      name.toLowerCase().includes(loweredFilter),
    );
  }, [entryNames, entryFilter]);

  const filteredSpecs = React.useMemo(() => {
    if (specFilter === "all") {
      return specifications;
    }

    return specifications.filter((specification) => specification === specFilter);
  }, [specifications, specFilter]);

  const totalCombinationCount = filteredEntries.length * filteredSpecs.length;
  const matchingEntryCount = filteredEntries.length;
  const maxRecordsPerRequest = Math.max(
    1,
    Math.min(serverInfo.api_limits.get_records || 1, 200)
  );
  const discoveryChunkSize = rowsPerPage * PREFETCH_PAGE_COUNT;

  const discoveryQueryKey = React.useMemo(
    () => [
      "datasetRecordDiscovery",
      datasetType,
      datasetId,
      rowsPerPage,
      entryFilter,
      specFilter,
      statusFilter,
    ],
    [
      datasetType,
      datasetId,
      rowsPerPage,
      entryFilter,
      specFilter,
      statusFilter,
    ],
  );

  const discoveryEnabled =
    !!datasetId &&
    !!datasetType &&
    filteredEntries.length > 0 &&
    filteredSpecs.length > 0;

  const discoveryQuery = useInfiniteQuery({
    queryKey: discoveryQueryKey,
    initialPageParam: INITIAL_CURSOR,
    enabled: discoveryEnabled,
    queryFn: async ({
      pageParam,
    }): Promise<DatasetRecordDiscoveryPage> => {
      const statuses =
        statusFilter === "all" ? undefined : [statusFilter];

      const rows: Omit<DiscoveredDatasetRecordRow, "status">[] = [];
      let specIndex = pageParam.specIndex;
      let entryOffset = pageParam.entryOffset;
      let scannedCount = pageParam.scannedCount;

      while (
        specIndex < filteredSpecs.length &&
        rows.length < discoveryChunkSize
      ) {
        const specificationName = filteredSpecs[specIndex];

        while (
          entryOffset < filteredEntries.length &&
          rows.length < discoveryChunkSize
        ) {
          const entryBatch = filteredEntries.slice(
            entryOffset,
            entryOffset + maxRecordsPerRequest,
          );

          const batch = await makeRequest<[string, string, number][]>(
            "POST",
            `api/v1/datasets/${datasetType}/${datasetId}/records/bulkFetch`,
            {
              entry_names: entryBatch,
              specification_names: [specificationName],
              status: statuses,
            },
          );

          const recordIdByEntry = new Map(
            batch.map(([entryName, , recordId]) => [entryName, recordId]),
          );

          for (const entryName of entryBatch) {
            const recordId = recordIdByEntry.get(entryName);
            if (recordId !== undefined) {
              rows.push({
                entry_name: entryName,
                specification_name: specificationName,
                record_id: recordId,
              });
            }
          }

          entryOffset += entryBatch.length;
          scannedCount += entryBatch.length;
        }

        if (entryOffset >= filteredEntries.length) {
          specIndex += 1;
          entryOffset = 0;
        }
      }

      const exhausted = specIndex >= filteredSpecs.length;

      if (rows.length === 0) {
        return {
          rows: [],
          exhausted,
          scannedCount,
          nextCursor: exhausted
            ? undefined
            : {
                specIndex,
                entryOffset,
                scannedCount,
              },
        };
      }

      const statusByRecordId: Record<number, qcpTypes.RecordStatus> = {};
      const recordIds = rows.map((row) => row.record_id);

      for (let i = 0; i < recordIds.length; i += maxRecordsPerRequest) {
        const recordIdBatch = recordIds.slice(i, i + maxRecordsPerRequest);
        const recordsBatch = await makeRequest<qcpTypes.BaseRecord[]>(
          "POST",
          "api/v1/records/bulkGet",
          {
            ids: recordIdBatch,
            include: ["status"],
          },
        );

        recordsBatch.forEach((record) => {
          statusByRecordId[record.id] = record.status;
        });
      }

      return {
        rows: rows.map((row) => {
          const status = statusByRecordId[row.record_id];
          if (!status) {
            throw new Error(`Missing status for record ${row.record_id}`);
          }

          return {
            ...row,
            status,
          };
        }),
        exhausted,
        scannedCount,
        nextCursor: exhausted
          ? undefined
          : {
              specIndex,
              entryOffset,
              scannedCount,
            },
      };
    },
    getNextPageParam: (lastPage) => lastPage.nextCursor,
  });

  const discoveredRows = React.useMemo(
    () => discoveryQuery.data?.pages.flatMap((discoveryPage) => discoveryPage.rows) ?? [],
    [discoveryQuery.data],
  );

  const lastDiscoveryPage = discoveryQuery.data?.pages[
    discoveryQuery.data.pages.length - 1
  ];
  const isExhausted = lastDiscoveryPage?.exhausted ?? totalCombinationCount === 0;
  const scannedCount = lastDiscoveryPage?.scannedCount ?? 0;
  const targetDiscoveredRows = (page + PREFETCH_PAGE_COUNT) * rowsPerPage;

  React.useEffect(() => {
    if (
      !discoveryQuery.isSuccess ||
      discoveryQuery.isFetchingNextPage ||
      !discoveryQuery.hasNextPage ||
      isExhausted
    ) {
      return;
    }

    if (discoveredRows.length >= targetDiscoveredRows) {
      return;
    }

    void discoveryQuery.fetchNextPage();
  }, [
    discoveredRows.length,
    discoveryQuery,
    isExhausted,
    targetDiscoveredRows,
  ]);

  React.useEffect(() => {
    if (!isExhausted || page === 0) {
      return;
    }

    const maxPage = Math.max(0, Math.ceil(discoveredRows.length / rowsPerPage) - 1);
    if (page > maxPage) {
      onPageChange(maxPage);
    }
  }, [discoveredRows.length, isExhausted, onPageChange, page, rowsPerPage]);

  const paginatedRecords = React.useMemo(() => {
    const start = page * rowsPerPage;
    return discoveredRows.slice(start, start + rowsPerPage);
  }, [discoveredRows, page, rowsPerPage]);

  const totalDatasetStatuses = React.useMemo(
    () => calculateTotalStatusCounts(datasetStatus),
    [datasetStatus],
  );

  const selectedSpecificationRecordCount = React.useMemo(() => {
    return filteredSpecs.reduce((total, specificationName) => {
      const specificationStatuses = datasetStatus[specificationName];
      if (!specificationStatuses) {
        return total;
      }

      if (statusFilter !== "all") {
        return total + (specificationStatuses[statusFilter] || 0);
      }

      return (
        total +
        Object.values(specificationStatuses).reduce(
          (specificationTotal, statusCount) => specificationTotal + statusCount,
          0,
        )
      );
    }, 0);
  }, [datasetStatus, filteredSpecs, statusFilter]);

  const rowCount = React.useMemo(() => {
    if (totalCombinationCount === 0) {
      return 0;
    }

    if (entryFilter === "" && specFilter !== "all") {
      return selectedSpecificationRecordCount;
    }

    if (entryFilter !== "") {
      return isExhausted ? discoveredRows.length : UNKNOWN_TOTAL_COUNT;
    }

    if (statusFilter !== "all") {
      return totalDatasetStatuses[statusFilter] || 0;
    }

    if (typeof totalRecords === "number") {
      return totalRecords;
    }

    return isExhausted ? discoveredRows.length : UNKNOWN_TOTAL_COUNT;
  }, [
    discoveredRows.length,
    entryFilter,
    isExhausted,
    selectedSpecificationRecordCount,
    specFilter,
    statusFilter,
    totalCombinationCount,
    totalDatasetStatuses,
    totalRecords,
  ]);

  const rowHeight = 44;
  const tableBodyHeight = rowsPerPage * rowHeight;
  const isInitialLoading = discoveryEnabled && discoveryQuery.isPending;
  const hasNoRows = paginatedRecords.length === 0;
  const isLoadingVisiblePage =
    !isExhausted && discoveredRows.length < page * rowsPerPage + rowsPerPage;

  const progressValue =
    totalCombinationCount > 0
      ? Math.min(100, (scannedCount / totalCombinationCount) * 100)
      : 0;

  return (
    <Box>
      <Box
        sx={{
          mb: 3,
          display: "flex",
          gap: 2,
          flexWrap: "wrap",
          alignItems: "center",
        }}
      >
        <TextField
          size="small"
          label="Filter Entry Name"
          value={entryFilter}
          onChange={(event) => {
            onEntryFilterChange(event.target.value);
          }}
          sx={{ minWidth: 200 }}
        />

        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Specification</InputLabel>
          <Select
            value={specFilter}
            label="Specification"
            onChange={(event: SelectChangeEvent) => {
              onSpecFilterChange(event.target.value);
            }}
          >
            <MenuItem value="all">All Specifications</MenuItem>
            {specifications.map((specification) => (
              <MenuItem key={specification} value={specification}>
                {specification}
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Status</InputLabel>
          <Select
            value={statusFilter}
            label="Status"
            onChange={(event: SelectChangeEvent) => {
              onStatusFilterChange(
                event.target.value as "all" | qcpTypes.RecordStatus,
              );
            }}
          >
            <MenuItem value="all">All Statuses</MenuItem>
            <MenuItem value="complete">Complete</MenuItem>
            <MenuItem value="waiting">Waiting</MenuItem>
            <MenuItem value="running">Running</MenuItem>
            <MenuItem value="error">Error</MenuItem>
            <MenuItem value="cancelled">Cancelled</MenuItem>
            <MenuItem value="deleted">Deleted</MenuItem>
            <MenuItem value="invalid">Invalid</MenuItem>
          </Select>
        </FormControl>

        {(discoveryQuery.isFetching || discoveryQuery.isFetchingNextPage) &&
          totalCombinationCount > 0 && (
            <Box sx={{ flexGrow: 1, minWidth: 260 }}>
              <Box
                sx={{
                  display: "flex",
                  justifyContent: "space-between",
                  mb: 0.5,
                }}
              >
                <Typography variant="caption" color="text.secondary">
                  Scanned {scannedCount} / {totalCombinationCount} combinations
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {discoveredRows.length} records discovered
                </Typography>
              </Box>
              <LinearProgress variant="determinate" value={progressValue} />
            </Box>
          )}
      </Box>

      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Entry Name</TableCell>
              <TableCell>Specification</TableCell>
              <TableCell sx={{ width: 250 }}>Record ID</TableCell>
              <TableCell sx={{ width: 250 }}>Status</TableCell>
            </TableRow>
          </TableHead>
          <TableBody sx={{ height: tableBodyHeight }}>
            {isInitialLoading ? (
              <TableRow>
                <TableCell colSpan={4} sx={{ height: tableBodyHeight }}>
                  <LoadingIndicator />
                </TableCell>
              </TableRow>
            ) : discoveryQuery.isError ? (
              <TableRow>
                <TableCell colSpan={4} sx={{ height: tableBodyHeight }}>
                  <Typography color="error">
                    Error fetching dataset records
                  </Typography>
                </TableCell>
              </TableRow>
            ) : (
              <>
                {hasNoRows ? (
                  <TableRow sx={{ height: rowHeight }}>
                    <TableCell colSpan={4} align="center">
                      {isLoadingVisiblePage
                        ? "Loading records..."
                        : "No records found for this page/filter"}
                    </TableCell>
                  </TableRow>
                ) : (
                  paginatedRecords.map((record) => (
                    <TableRow
                      key={`${record.specification_name}-${record.entry_name}-${record.record_id}`}
                      sx={{ height: rowHeight }}
                    >
                      <TableCell>{record.entry_name}</TableCell>
                      <TableCell>{record.specification_name}</TableCell>
                      <TableCell sx={{ width: 120 }}>
                        <MuiLink to={`/records/${record.record_id}`}>
                          {record.record_id}
                        </MuiLink>
                      </TableCell>
                      <TableCell sx={{ width: 150 }}>
                        <StatusChip
                          status={record.status}
                          recordId={record.record_id}
                          recordType={datasetType}
                        />
                      </TableCell>
                    </TableRow>
                  ))
                )}
                {Array.from({
                  length:
                    paginatedRecords.length === 0
                      ? Math.max(0, rowsPerPage - 1)
                      : Math.max(0, rowsPerPage - paginatedRecords.length),
                }).map((_, index) => (
                  <TableRow key={`empty-${index}`} sx={{ height: rowHeight }}>
                    <TableCell colSpan={4} />
                  </TableRow>
                ))}
              </>
            )}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 25, 50]}
        component="div"
        count={rowCount}
        rowsPerPage={rowsPerPage}
        page={page}
        labelDisplayedRows={({ from, to, count }) => {
          if (count !== UNKNOWN_TOTAL_COUNT) {
            return `${from}-${to} of ${count}`;
          }

          if (entryFilter !== "") {
            return `${from}-${to} of more than ${to} (${matchingEntryCount} matching entries)`;
          }

          return `${from}-${to} of more than ${to}`;
        }}
        onPageChange={(_event, newPage) => onPageChange(newPage)}
        onRowsPerPageChange={(event) => {
          onRowsPerPageChange(parseInt(event.target.value, 10));
        }}
      />
    </Box>
  );
}
