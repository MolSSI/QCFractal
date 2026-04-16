import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import React, { useMemo, useState } from "react";
import { ServerErrorFragment } from "../components/ServerErrorFragment.tsx";
import * as qcpTypes from "../PortalTypes.ts";
import {
  Box,
  Collapse,
  Grid,
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
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import LoadingIndicator from "../components/LoadingIndicator.tsx";
import ErrorIndicator from "../components/ErrorIndicator.tsx";
import { parseToDate, truncateFront } from "../Utils.ts";
import { useAuth } from "../Auth.tsx";

const ServerErrorRow: React.FC<{ errorLog: qcpTypes.ServerErrorLog }> = ({
  errorLog,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);

  const handleToggleExpand = () => {
    setIsExpanded((prev) => !prev);
  };

  return (
    <React.Fragment>
      <TableRow hover onClick={handleToggleExpand} sx={{ cursor: "pointer" }}>
        <TableCell width="50px">
          <IconButton size="small">
            {isExpanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell>
          <Typography variant="body2" fontWeight="bold">
            {errorLog.id}
          </Typography>
        </TableCell>
        <TableCell>
          <Typography variant="body2">
            {parseToDate(errorLog.error_date)?.toLocaleString()}
          </Typography>
        </TableCell>
        <TableCell>
          <Typography variant="body2">
            {errorLog.user || "(anonymous)"}
          </Typography>
        </TableCell>
        <TableCell>
          <Typography
            variant="body2"
            sx={{
              maxWidth: "400px",
              whiteSpace: "nowrap",
            }}
          >
            {truncateFront(errorLog.error_text, 90)}
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={5}>
          <Collapse in={isExpanded} timeout="auto" unmountOnExit>
            <Box mt={3} mb={3} ml={8}>
              <ServerErrorFragment errorLog={errorLog} />
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

export default function ServerErrorList() {
  const { makeRequest } = usePortalClient();
  const { serverInfo } = useAuth();

  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(
    serverInfo.api_limits.get_error_logs || 100,
  );
  const [filter, setFilter] = useState("");

  const serverErrorQueryBody = useMemo<qcpTypes.ServerErrorLogQueryFilters>(
    () => ({
      limit: rowsPerPage,
      // cursor is handled by pagination in a real world scenario,
      // but here we just fetch the first 'limit' errors for simplicity,
      // matching the InternalJobList behavior.
    }),
    [rowsPerPage],
  );

  const {
    status,
    data: serverErrorData,
    error,
  } = useQuery({
    queryKey: ["listServerErrors", serverErrorQueryBody],
    queryFn: () =>
      makeRequest<qcpTypes.ServerErrorLog[]>(
        "POST",
        "/api/v1/server_errors/query",
        serverErrorQueryBody,
      ),
  });

  const filteredErrors = useMemo(() => {
    if (!serverErrorData) return [];
    return serverErrorData.filter(
      (e) =>
        e.error_text.toLowerCase().includes(filter.toLowerCase()) ||
        e.id.toString().includes(filter) ||
        (e.user && e.user.toLowerCase().includes(filter.toLowerCase())),
    );
  }, [serverErrorData, filter]);

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

  if (status == "pending") {
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
        <LoadingIndicator />
      </Box>
    );
  }

  if (status == "error") {
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
        <ErrorIndicator message={error.message} />
      </Box>
    );
  }

  return (
    <Grid container spacing={2} width="100%">
      <Grid size={12}>
        <Typography variant="h4" marginBottom={3}>
          Server Errors
        </Typography>
        <Box width={"30%"} mb={2}>
          <TextField
            fullWidth
            variant="outlined"
            size="small"
            label="Filter errors"
            value={filter}
            onChange={handleFilterChange}
          />
        </Box>

        {filteredErrors && filteredErrors.length > 0 ? (
          <>
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell width="50px" />
                    <TableCell width="10%">ID</TableCell>
                    <TableCell width="20%">Date</TableCell>
                    <TableCell width="15%">User</TableCell>
                    <TableCell>Error Text</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredErrors
                    .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                    .map((errorLog) => (
                      <ServerErrorRow key={errorLog.id} errorLog={errorLog} />
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
            <TablePagination
              rowsPerPageOptions={[
                10,
                20,
                50,
                100,
                serverInfo.api_limits.get_error_logs,
              ]
                .filter((x) => x > 0)
                .sort((a, b) => a - b)}
              component="div"
              count={filteredErrors.length}
              rowsPerPage={rowsPerPage}
              page={page}
              onPageChange={handleChangePage}
              onRowsPerPageChange={handleChangeRowsPerPage}
            />
          </>
        ) : (
          <Typography variant="body1">
            {filter
              ? "(no errors match the filter)"
              : "(no server errors found)"}
          </Typography>
        )}
      </Grid>
    </Grid>
  );
}
