import {
  Button,
  FormControl,
  InputLabel,
  Link as MuiLink,
  MenuItem,
  Paper,
  Select,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Typography,
} from "@mui/material";
import React from "react";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes.ts";
import LoadingIndicator from "./LoadingIndicator.tsx";
import ErrorIndicator from "./ErrorIndicator.tsx";
import { RecordTypeChip } from "./RecordTypeChip.tsx";
import { StatusChip } from "./StatusChip.tsx";
import { dateStringToLocalTime } from "../Utils.ts";

// A single compute attempt (run) made by this manager on a record.
type ManagerRun = {
  historyId: number;
  recordId: number;
  recordType: qcpTypes.RecordType;
  status: string;
  modifiedOn: string;
};

const RECORD_STATUSES: qcpTypes.RecordStatus[] = [
  "complete",
  "waiting",
  "running",
  "error",
  "cancelled",
  "deleted",
  "invalid",
];

// Display names consistent with the manager pie chart ("active", "success",
// "failed"); other statuses fall back to their capitalized status name.
const STATUS_LABELS: Partial<Record<qcpTypes.RecordStatus, string>> = {
  running: "Active",
  complete: "Success",
  error: "Failed",
};

const statusLabel = (s: qcpTypes.RecordStatus) =>
  STATUS_LABELS[s] ?? s.charAt(0).toUpperCase() + s.slice(1);

// Number of per-record compute_history requests to run in parallel.
const COMPUTE_HISTORY_CONCURRENCY = 10;

interface ManagerTaskHistoryProps {
  managerName: string;
}

export const ManagerTaskHistory: React.FC<ManagerTaskHistoryProps> = ({
  managerName,
}) => {
  const { makeRequest } = usePortalClient();

  // "All tasks on this manager" — every compute attempt this manager made, at
  // the attempt level (a record can be run/failed multiple times), so the
  // counts match the manager's success/failure tallies. This is expensive: a
  // slow history_manager_name query plus a compute_history fetch per record,
  // so it only runs on demand when the user clicks "Load All Tasks". There is
  // no record cap; the per-record fetches run in bounded-concurrency batches so
  // this scales to managers with arbitrarily many records.
  const [historyEnabled, setHistoryEnabled] = React.useState(false);

  const {
    status: historyStatus,
    data: historyAttempts,
    error: historyError,
  } = useQuery({
    queryKey: ["managerHistoryAttempts", managerName],
    queryFn: async (): Promise<ManagerRun[]> => {
      const recordIds = await makeRequest<number[]>(
        "POST",
        `api/v1/records/query`,
        {
          history_manager_name: [managerName],
        },
      );
      if (recordIds.length === 0) {
        return [];
      }
      const records = await makeRequest<qcpTypes.BaseRecord[]>(
        "POST",
        `api/v1/records/bulkGet`,
        {
          ids: recordIds,
        },
      );
      // Fetch each record's compute history, keeping only the attempts made by
      // this manager. Run in fixed-size batches so we never fire thousands of
      // requests at once for very large managers.
      const histories: {
        record: qcpTypes.BaseRecord;
        entries: qcpTypes.ComputeHistory[];
      }[] = [];
      for (let i = 0; i < records.length; i += COMPUTE_HISTORY_CONCURRENCY) {
        const batch = records.slice(i, i + COMPUTE_HISTORY_CONCURRENCY);
        const batchResults = await Promise.all(
          batch.map((r) =>
            makeRequest<qcpTypes.ComputeHistory[]>(
              "GET",
              `api/v1/records/${r.record_type}/${r.id}/compute_history`,
            )
              .then((entries) => ({ record: r, entries }))
              .catch(() => ({
                record: r,
                entries: [] as qcpTypes.ComputeHistory[],
              })),
          ),
        );
        histories.push(...batchResults);
      }
      const runs: ManagerRun[] = [];
      for (const { record, entries } of histories) {
        for (const e of entries) {
          if (e.manager_name === managerName) {
            runs.push({
              historyId: e.id,
              recordId: record.id,
              recordType: record.record_type,
              status: e.status,
              modifiedOn: e.modified_on,
            });
          }
        }
      }
      runs.sort((a, b) => (a.modifiedOn < b.modifiedOn ? 1 : -1));
      return runs;
    },
    enabled: !!managerName && historyEnabled,
    staleTime: Infinity, // don't auto-refetch the expensive query
  });

  const [historyStatusFilter, setHistoryStatusFilter] =
    React.useState<string>("all");
  const [historyPage, setHistoryPage] = React.useState(0);
  const [historyRowsPerPage, setHistoryRowsPerPage] = React.useState(10);

  const historyStatusCounts = React.useMemo(() => {
    const counts: Record<string, number> = {};
    (historyAttempts ?? []).forEach((r) => {
      counts[r.status] = (counts[r.status] ?? 0) + 1;
    });
    return counts;
  }, [historyAttempts]);

  const filteredHistoryRuns = React.useMemo(() => {
    if (!historyAttempts) return [];
    if (historyStatusFilter === "all") return historyAttempts;
    return historyAttempts.filter((r) => r.status === historyStatusFilter);
  }, [historyAttempts, historyStatusFilter]);

  return (
    <>
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        flexWrap="wrap"
        gap={2}
        sx={{ mb: 2 }}
      >
        <Typography variant="h6">All Tasks on This Manager</Typography>
        {historyStatus === "success" && historyAttempts && (
          <FormControl size="small" sx={{ minWidth: 220 }}>
            <InputLabel id="history-status-filter-label">
              Filter by status
            </InputLabel>
            <Select
              labelId="history-status-filter-label"
              label="Filter by status"
              value={historyStatusFilter}
              onChange={(e) => {
                setHistoryStatusFilter(e.target.value);
                setHistoryPage(0);
              }}
            >
              <MenuItem value="all">All ({historyAttempts.length})</MenuItem>
              {RECORD_STATUSES.filter(
                (s) => (historyStatusCounts[s] ?? 0) > 0,
              ).map((s) => (
                <MenuItem key={s} value={s}>
                  {statusLabel(s)} ({historyStatusCounts[s]})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        )}
      </Stack>

      {!historyEnabled && (
        <Stack spacing={1} alignItems="flex-start">
          <Typography variant="body2" color="text.secondary">
            Lists every compute attempt this manager has made (each run of a
            record, not just currently claimed ones), so counts match the
            manager's success/failure tallies. This is a heavy query and can
            take a minute or more.
          </Typography>
          <Button variant="outlined" onClick={() => setHistoryEnabled(true)}>
            Load All Tasks
          </Button>
        </Stack>
      )}

      {historyEnabled && historyStatus === "pending" && (
        <Stack spacing={1} alignItems="center" sx={{ py: 2 }}>
          <LoadingIndicator />
          <Typography variant="body2" color="text.secondary">
            Loading run history — this may take a minute or more…
          </Typography>
        </Stack>
      )}

      {historyEnabled && historyStatus === "error" && (
        <ErrorIndicator message={historyError.message} />
      )}

      {historyEnabled && historyStatus === "success" && historyAttempts && (
        <>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            Note: these counts may differ from the chart above. The chart is a
            running tally of every task the manager has ever returned, while
            this list is rebuilt from records that still exist — deleted records
            are counted in the chart but not shown here.
          </Typography>
          <TableContainer component={Paper} variant="outlined">
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>
                    <strong>Record ID</strong>
                  </TableCell>
                  <TableCell>
                    <strong>Type</strong>
                  </TableCell>
                  <TableCell>
                    <strong>Status</strong>
                  </TableCell>
                  <TableCell>
                    <strong>Run time</strong>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {filteredHistoryRuns.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={4} align="center">
                      No runs found for this manager
                      {historyStatusFilter !== "all"
                        ? ` with status "${statusLabel(
                            historyStatusFilter as qcpTypes.RecordStatus,
                          )}"`
                        : ""}
                      .
                    </TableCell>
                  </TableRow>
                ) : (
                  filteredHistoryRuns
                    .slice(
                      historyPage * historyRowsPerPage,
                      historyPage * historyRowsPerPage + historyRowsPerPage,
                    )
                    .map((run) => (
                      <TableRow key={run.historyId}>
                        <TableCell>
                          <MuiLink
                            to={`/records/${run.recordId}`}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {run.recordId}
                          </MuiLink>
                        </TableCell>
                        <TableCell>
                          <RecordTypeChip type={run.recordType} />
                        </TableCell>
                        <TableCell>
                          <StatusChip
                            status={run.status}
                            recordType={run.recordType}
                            recordId={run.recordId}
                            computeHistoryId={run.historyId}
                          />
                        </TableCell>
                        <TableCell>
                          {dateStringToLocalTime(run.modifiedOn)}
                        </TableCell>
                      </TableRow>
                    ))
                )}
              </TableBody>
            </Table>
          </TableContainer>
          {filteredHistoryRuns.length > 0 && (
            <TablePagination
              rowsPerPageOptions={[10, 25, 50]}
              component="div"
              count={filteredHistoryRuns.length}
              rowsPerPage={historyRowsPerPage}
              page={historyPage}
              onPageChange={(_e, newPage) => setHistoryPage(newPage)}
              onRowsPerPageChange={(e) => {
                setHistoryRowsPerPage(parseInt(e.target.value, 10));
                setHistoryPage(0);
              }}
            />
          )}
        </>
      )}
    </>
  );
};

export default ManagerTaskHistory;
