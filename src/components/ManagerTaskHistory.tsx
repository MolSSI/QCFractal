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

// A single compute attempt (run) made by this manager on a record. Finished
// attempts come from compute_history (with a historyId); currently-running
// tasks have no history entry yet, so historyId is undefined for those.
type ManagerRun = {
  key: string;
  historyId?: number;
  recordId: number;
  recordType: qcpTypes.RecordType;
  status: string;
  modifiedOn: string;
};

// Filter order: the three primary pie-chart buckets first (active, success,
// failed), then the remaining statuses.
const RECORD_STATUSES: qcpTypes.RecordStatus[] = [
  "running",
  "complete",
  "error",
  "waiting",
  "cancelled",
  "deleted",
  "invalid",
];

// The primary buckets are always shown (even at 0) so they line up with the
// pie chart; the rest only appear when present in the data.
const ALWAYS_SHOWN_STATUSES: qcpTypes.RecordStatus[] = [
  "running",
  "complete",
  "error",
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
  // slow history_manager_name query, so it only runs on demand when the user
  // clicks "Load All Tasks".
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
      // Pull the records together with their full compute_history in a single
      // bulk call (include ["*", "compute_history"]), then keep only the
      // attempts made by this manager. This avoids a separate per-record
      // history request.
      const records = await makeRequest<qcpTypes.BaseRecord[]>(
        "POST",
        `api/v1/records/bulkGet`,
        {
          ids: recordIds,
          include: ["*", "compute_history"],
        },
      );
      const runs: ManagerRun[] = [];
      for (const record of records) {
        for (const e of record.compute_history ?? []) {
          if (e.manager_name === managerName) {
            runs.push({
              key: `h-${e.id}`,
              historyId: e.id,
              recordId: record.id,
              recordType: record.record_type,
              status: e.status,
              modifiedOn: e.modified_on,
            });
          }
        }
      }
      // Currently-running tasks claimed by this manager have no finished
      // compute_history entry yet, so fetch them separately and add a row for
      // each in-progress run.
      const runningIds = await makeRequest<number[]>(
        "POST",
        `api/v1/records/query`,
        {
          manager_name: [managerName],
          status: ["running"],
        },
      );
      if (runningIds.length > 0) {
        const runningRecords = await makeRequest<qcpTypes.BaseRecord[]>(
          "POST",
          `api/v1/records/bulkGet`,
          {
            ids: runningIds,
          },
        );
        for (const record of runningRecords) {
          runs.push({
            key: `r-${record.id}`,
            recordId: record.id,
            recordType: record.record_type,
            status: record.status,
            modifiedOn: record.modified_on,
          });
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
                (s) =>
                  ALWAYS_SHOWN_STATUSES.includes(s) ||
                  (historyStatusCounts[s] ?? 0) > 0,
              ).map((s) => (
                <MenuItem key={s} value={s}>
                  {statusLabel(s)} ({historyStatusCounts[s] ?? 0})
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
            record) This is a heavy query and can take a minute or more.
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
                      <TableRow key={run.key}>
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
