import {
  Box,
  Chip,
  Grid,
  Paper,
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
import { usePortalClient } from "../PortalClient.tsx";
import React from "react";
import * as qcpTypes from "../PortalTypes.ts";
import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import LoadingIndicator from "../components/LoadingIndicator.tsx";
import ErrorIndicator from "../components/ErrorIndicator.tsx";
import { RecordTypeChip } from "../components/RecordTypeChip.tsx";
import { StatusChip } from "../components/StatusChip.tsx";
import { ManagerPieChart } from "../components/ManagerPieChart.tsx";
import { dateStringToLocalTime } from "../Utils.ts";

export default function Manager() {
  const { managerName } = useParams();

  const { makeRequest } = usePortalClient();

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
    data: activeRecords,
    error: recordsError,
  } = useQuery({
    queryKey: ["managerActiveRecords", managerName],
    queryFn: async () => {
      const recordIds = await makeRequest<number[]>(
        "POST",
        `api/v1/records/query`,
        {
          manager_name: [managerName],
          status: ["running"],
        },
      );
      if (recordIds.length === 0) {
        return [];
      }
      return makeRequest<qcpTypes.BaseRecord[]>(
        "POST",
        `api/v1/records/bulkGet`,
        {
          ids: recordIds,
        },
      );
    },
    enabled: !!managerName && managerData?.status === "active",
  });

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
                                <Link
                                  to={`/records/${record.id}`}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  style={{
                                    color: "inherit",
                                    textDecoration: "underline",
                                  }}
                                >
                                  {record.id}
                                </Link>
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
        </Grid>
      )}
    </>
  );
}
