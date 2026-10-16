import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import React, { useMemo, useState } from "react";
import { usePageTitle } from "../UsePageTitle.ts";
import { InternalJobFragment } from "../components/InternalJobFragment.tsx";
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
import { dateStringToLocalTime } from "../Utils.ts";
import { InternalJobStatusChip } from "../components/InternalJobStatusChip.tsx";

const InternalJobRow: React.FC<{ job: qcpTypes.InternalJob }> = ({ job }) => {
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
            {job.name}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            ID: {job.id}
          </Typography>
        </TableCell>
        <TableCell>
          <InternalJobStatusChip status={job.status} />
        </TableCell>
        <TableCell>
          <Typography variant="body2">
            <strong>Added:</strong> {dateStringToLocalTime(job.added_date)}
          </Typography>
          {job.started_date && (
            <Typography variant="body2" color="text.secondary">
              <strong>Started:</strong>{" "}
              {dateStringToLocalTime(job.started_date)}
            </Typography>
          )}
        </TableCell>
        <TableCell>
          <Typography variant="body2">{job.progress.toFixed(0)}%</Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={5}>
          <Collapse in={isExpanded} timeout="auto" unmountOnExit>
            <Box mt={3} mb={3} ml={8}>
              <InternalJobFragment internalJobId={job.id} />
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

export default function InternalJobList() {
  usePageTitle("Internal Jobs");
  const internalJobQueryBody = useMemo(
    () => ({
      status: ["waiting", "running"],
      limit: 1000,
    }),
    [],
  );

  const { makeRequest } = usePortalClient();

  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(20);
  const [filter, setFilter] = useState("");

  const {
    status,
    data: internalJobData,
    error,
  } = useQuery({
    queryKey: ["listInternalJobs"],
    queryFn: () =>
      makeRequest<qcpTypes.InternalJob[]>(
        "POST",
        "/api/v1/internal_jobs/query",
        internalJobQueryBody,
      ),
  });

  const filteredJobs = useMemo(() => {
    if (!internalJobData) return [];
    return internalJobData.filter(
      (j) =>
        j.name.toLowerCase().includes(filter.toLowerCase()) ||
        j.id.toString().includes(filter),
    );
  }, [internalJobData, filter]);

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
          Internal Jobs
        </Typography>
        <Box width={"30%"} mb={2}>
          <TextField
            fullWidth
            variant="outlined"
            size="small"
            label="Filter jobs"
            value={filter}
            onChange={handleFilterChange}
          />
        </Box>

        {filteredJobs && filteredJobs.length > 0 ? (
          <>
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell width="50px" />
                    <TableCell width="40%">Name / ID</TableCell>
                    <TableCell width="15%">Status</TableCell>
                    <TableCell width="30%">Dates</TableCell>
                    <TableCell width="10%">Progress</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredJobs
                    .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                    .map((job) => (
                      <InternalJobRow key={job.id} job={job} />
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
            <TablePagination
              rowsPerPageOptions={[10, 20, 50, 100]}
              component="div"
              count={filteredJobs.length}
              rowsPerPage={rowsPerPage}
              page={page}
              onPageChange={handleChangePage}
              onRowsPerPageChange={handleChangeRowsPerPage}
            />
          </>
        ) : (
          <Typography variant="body1">
            {filter
              ? "(no jobs match the filter)"
              : "(no waiting or running internal jobs)"}
          </Typography>
        )}
      </Grid>
    </Grid>
  );
}
