import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Grid,
  IconButton,
  Link as MuiLink,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import RefreshIcon from "@mui/icons-material/Refresh";
import { usePortalClient } from "../PortalClient.tsx";
import { useAuth } from "../Auth.tsx";
import React from "react";
import { usePageTitle } from "../UsePageTitle.ts";
import * as qcpTypes from "../PortalTypes.ts";
import { useParams } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import LoadingIndicator from "../components/LoadingIndicator.tsx";
import ErrorIndicator from "../components/ErrorIndicator.tsx";
import { RecordTypeChip } from "../components/RecordTypeChip.tsx";
import { StatusChip } from "../components/StatusChip.tsx";
import { ManagerPieChart } from "../components/ManagerPieChart.tsx";
import { ManagerTaskHistory } from "../components/ManagerTaskHistory.tsx";
import { dateStringToLocalTime } from "../Utils.ts";

// Minimum time the loading spinner replaces the refresh icon. A refresh slower
// than this keeps spinning until it finishes.
const REFRESH_SPINNER_MIN_MS = 300;

export default function Manager() {
  const { managerName } = useParams();

  usePageTitle(`Manager: ${managerName}`);

  const { makeRequest } = usePortalClient();
  const { serverInfo } = useAuth();
  const queryClient = useQueryClient();

  // records/query is not bounded by the server's api limits, but bulkGet is:
  // asking for more than get_records at once fails with
  // "Cannot get N records - limit is M". A busy manager can easily claim more
  // than that, so only the first maxClaimedRecords ids are fetched and the
  // table says so.
  const maxClaimedRecords = Math.max(1, serverInfo.api_limits.get_records || 1);

  const {
    status,
    data: managerData,
    error,
  } = useQuery({
    queryKey: ["managerInfo", managerName],
    queryFn: () =>
      makeRequest<qcpTypes.Manager>("GET", `api/v1/managers/${managerName}`),
    enabled: !!managerName,
  });

  const {
    status: recordsStatus,
    data: activeRecordsResult,
    error: recordsError,
  } = useQuery({
    queryKey: ["managerActiveRecords", managerName, maxClaimedRecords],
    queryFn: async () => {
      const recordIds = await makeRequest<number[]>(
        "POST",
        `api/v1/records/query`,
        {
          manager_name: [managerName],
          status: ["running"],
        },
      );
      // Keep the full tally so the table can report how many were left out.
      const totalCount = recordIds.length;
      const cappedIds = recordIds.slice(0, maxClaimedRecords);
      if (cappedIds.length === 0) {
        return { records: [] as qcpTypes.BaseRecord[], totalCount };
      }
      const records = await makeRequest<qcpTypes.BaseRecord[]>(
        "POST",
        `api/v1/records/bulkGet`,
        {
          ids: cappedIds,
        },
      );
      return { records, totalCount };
    },
    enabled: !!managerName && managerData?.status === "active",
  });

  const activeRecords = activeRecordsResult?.records;
  const totalClaimedRecords = activeRecordsResult?.totalCount ?? 0;
  const isClaimedRecordsTruncated =
    totalClaimedRecords > (activeRecords?.length ?? 0);

  const [isRefreshing, setIsRefreshing] = React.useState(false);

  // Refreshes the manager itself (status, last seen, task tallies) and the
  // claimed-records table. The "All Tasks on This Manager" history is left
  // alone on purpose: it is an expensive on-demand query with its own load
  // button, so it should not be re-run by a page-level refresh.
  const handleRefresh = async () => {
    // Ignore clicks while a refresh is still spinning rather than disabling
    // the button, so the icon keeps its normal color during the animation.
    if (!managerName || isRefreshing) {
      return;
    }

    setIsRefreshing(true);
    try {
      await Promise.all([
        ...[
          ["managerInfo", managerName],
          ["managerActiveRecords", managerName],
        ].map((queryKey) => queryClient.invalidateQueries({ queryKey })),
        // The refetches usually finish too quickly to notice, so hold the
        // spinner for a moment to confirm the click actually registered.
        new Promise((resolve) => setTimeout(resolve, REFRESH_SPINNER_MIN_MS)),
      ]);
    } finally {
      setIsRefreshing(false);
    }
  };

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

  if (!managerName) {
    return (
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          minHeight: "70vh",
          width: "100%",
        }}
      >
        <ErrorIndicator message="Missing manager name" />
      </Box>
    );
  }

  return (
    <>
      {status === "pending" && (
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "70vh",
            width: "100%",
          }}
        >
          <LoadingIndicator />
        </Box>
      )}

      {status === "error" && (
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "70vh",
            width: "100%",
          }}
        >
          <ErrorIndicator message={error.message} />
        </Box>
      )}

      {status === "success" && managerData && (
        <Grid container spacing={3} width="100%">
          <Grid size={12}>
            <Box sx={{ display: "flex", alignItems: "center" }}>
              <Stack>
                <Typography variant="h4" fontWeight="bold">
                  {managerData.name}
                </Typography>
                <Chip
                  label={managerData.status}
                  color={managerData.status == "active" ? "success" : "default"}
                  sx={{ width: "fit-content", fontWeight: "bold" }}
                />
              </Stack>
              <Box sx={{ ml: "auto" }}>
                <Tooltip title="Refresh manager information">
                  <IconButton onClick={handleRefresh} color="primary">
                    {isRefreshing ? (
                      // Same 24px footprint as RefreshIcon, so swapping the
                      // two does not shift the button or the header row.
                      <CircularProgress size={24} color="inherit" />
                    ) : (
                      <RefreshIcon />
                    )}
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>
          </Grid>
          <Grid size={4}>
            <Typography variant="body1">
              <strong>Manager version:</strong> {managerData.manager_version}
            </Typography>
            <Typography variant="body1">
              <strong>Created:</strong>{" "}
              {dateStringToLocalTime(managerData.created_on)}
            </Typography>
            <Typography variant="body1">
              <strong>Last seen:</strong>{" "}
              {dateStringToLocalTime(managerData.modified_on)}
            </Typography>
            <Typography variant="body1">
              <strong>Cluster:</strong> {managerData.cluster}
            </Typography>
            <Typography variant="body1">
              <strong>Hostname:</strong> {managerData.hostname}
            </Typography>
          </Grid>
          <Grid size={3}>
            <Typography variant="h6">Tags</Typography>
            <ul>
              {managerData.tags.map((tag, index) => (
                <li key={index}>{tag}</li>
              ))}
            </ul>
          </Grid>
          <Grid size={5}>
            <ManagerPieChart managerData={managerData} />
          </Grid>
          <Grid size={5}>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Available Programs/Versions
            </Typography>
            <TableContainer component={Paper}>
              <Table size="small" aria-label="manager programs table">
                <TableHead>
                  <TableRow>
                    <TableCell>
                      <strong>Program</strong>
                    </TableCell>
                    <TableCell>
                      <strong>Versions</strong>
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {Object.entries(managerData.programs).map(([p, v]) => {
                    return (
                      <React.Fragment key={p}>
                        <TableRow>
                          <TableCell>{p}</TableCell>
                          <TableCell>{v ? v : "?"}</TableCell>
                        </TableRow>
                      </React.Fragment>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          </Grid>

          <Grid size={7}>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Currently Claimed Records
            </Typography>
            {managerData.status === "active" && recordsStatus === "pending" && (
              <LoadingIndicator />
            )}
            {recordsStatus === "error" && (
              <ErrorIndicator message={recordsError.message} />
            )}
            {managerData.status !== "active" && (
              <Typography variant="body1">Manager is not active</Typography>
            )}
            {recordsStatus === "success" && activeRecords && (
              <>
                {isClaimedRecordsTruncated && (
                  <Alert severity="info" sx={{ mb: 2 }}>
                    This manager has claimed {totalClaimedRecords} records, but
                    the server returns at most {maxClaimedRecords} per request.
                    Showing only the top {activeRecords.length} of{" "}
                    {totalClaimedRecords} records.
                  </Alert>
                )}
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>
                          <strong>ID</strong>
                        </TableCell>
                        <TableCell>
                          <strong>Type</strong>
                        </TableCell>
                        <TableCell>
                          <strong>Status</strong>
                        </TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {activeRecords.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={4} align="center">
                            No active records for this manager.
                          </TableCell>
                        </TableRow>
                      ) : (
                        activeRecords
                          .slice(
                            page * rowsPerPage,
                            page * rowsPerPage + rowsPerPage,
                          )
                          .map((record) => (
                            <TableRow key={record.id}>
                              <TableCell>
                                <MuiLink
                                  to={`/records/${record.id}`}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                >
                                  {record.id}
                                </MuiLink>
                              </TableCell>
                              <TableCell>
                                <RecordTypeChip type={record.record_type} />
                              </TableCell>
                              <TableCell>
                                <StatusChip
                                  status={record.status}
                                  recordType={record.record_type}
                                  recordId={record.id}
                                />
                              </TableCell>
                            </TableRow>
                          ))
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>
                {activeRecords.length > 0 && (
                  <TablePagination
                    rowsPerPageOptions={[10, 25, 50]}
                    component="div"
                    count={activeRecords.length}
                    rowsPerPage={rowsPerPage}
                    page={page}
                    onPageChange={handleChangePage}
                    onRowsPerPageChange={handleChangeRowsPerPage}
                  />
                )}
              </>
            )}
          </Grid>

          <Grid size={12}>
            <ManagerTaskHistory managerName={managerName} />
          </Grid>
        </Grid>
      )}
    </>
  );
}
