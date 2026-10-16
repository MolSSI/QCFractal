import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import React, { useMemo, useState } from "react";
import { usePageTitle } from "../UsePageTitle.ts";
import { ManagerFragment } from "../components/ManagerFragment.tsx";
import * as qcpTypes from "../PortalTypes.ts";
import {
  Box,
  Button,
  Collapse,
  Grid,
  IconButton,
  Paper,
  Stack,
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
import { PieChart } from "@mui/x-charts/PieChart";
import { Link } from "react-router-dom";

const ManagerRow: React.FC<{ manager: qcpTypes.Manager }> = ({ manager }) => {
  const [isExpanded, setIsExpanded] = useState(false);

  const handleToggleExpand = () => {
    setIsExpanded((prev) => !prev);
  };

  const returned =
    manager.claimed -
    (manager.active_tasks + manager.successes + manager.failures);

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
            {manager.name}
          </Typography>
        </TableCell>
        <TableCell>{manager.cluster}</TableCell>
        <TableCell>
          <Typography variant="body2">
            Created: {dateStringToLocalTime(manager.created_on)}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Updated: {dateStringToLocalTime(manager.modified_on)}
          </Typography>
        </TableCell>
        <TableCell>
          <Stack direction="row" spacing={1} alignItems="center">
            <Typography
              variant="body2"
              sx={{ width: "40px", textAlign: "right" }}
            >
              {manager.claimed}
            </Typography>
            <PieChart
              skipAnimation={true}
              width={40}
              height={40}
              hideLegend={true}
              colors={["blue", "green", "red", "orange"]}
              series={[
                {
                  data: [
                    { id: 0, value: manager.active_tasks, label: "active" },
                    { id: 1, value: manager.successes, label: "success" },
                    { id: 2, value: manager.failures, label: "failed" },
                    { id: 3, value: returned, label: "returned" },
                  ],
                },
              ]}
            />
          </Stack>
        </TableCell>
        <TableCell>
          <Button
            variant="contained"
            size="small"
            component={Link}
            to={`/managers/${manager.name}`}
            onClick={(e) => {
              e.stopPropagation();
            }}
          >
            View
          </Button>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
          <Collapse in={isExpanded} timeout="auto" unmountOnExit>
            <Box mt={3} mb={3} ml={8}>
              <ManagerFragment managerName={manager.name} />
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

type TagClusterCount = { tag: string; cluster: string; count: number };

const ComputeTagSummary: React.FC<{ managers: qcpTypes.Manager[] }> = ({
  managers,
}) => {
  const rows = useMemo(() => {
    const counts = new Map<string, TagClusterCount>();
    for (const manager of managers) {
      for (const tag of manager.tags ?? []) {
        const key = JSON.stringify([tag, manager.cluster]);
        const row = counts.get(key);
        if (row) {
          row.count += 1;
        } else {
          counts.set(key, { tag, cluster: manager.cluster, count: 1 });
        }
      }
    }
    return [...counts.values()].sort(
      (a, b) =>
        a.tag.localeCompare(b.tag) || a.cluster.localeCompare(b.cluster),
    );
  }, [managers]);

  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  if (rows.length === 0) return null;

  const lastPage = Math.max(0, Math.ceil(rows.length / rowsPerPage) - 1);
  const currentPage = Math.min(page, lastPage);

  return (
    <Box mb={4} maxWidth={600}>
      <Typography variant="h6">Compute Tags by Cluster</Typography>
      <Typography variant="body2" color="text.secondary" mb={1}>
        A manager is counted once for each compute tag it serves.
      </Typography>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Compute Tag</TableCell>
              <TableCell>Cluster</TableCell>
              <TableCell align="right">Count</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {rows
              .slice(
                currentPage * rowsPerPage,
                currentPage * rowsPerPage + rowsPerPage,
              )
              .map((row) => (
                <TableRow key={JSON.stringify([row.tag, row.cluster])} hover>
                  <TableCell>{row.tag}</TableCell>
                  <TableCell>{row.cluster}</TableCell>
                  <TableCell align="right">{row.count}</TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 20, 50]}
        component="div"
        count={rows.length}
        rowsPerPage={rowsPerPage}
        page={currentPage}
        onPageChange={(_event, newPage) => setPage(newPage)}
        onRowsPerPageChange={(event) => {
          setRowsPerPage(parseInt(event.target.value, 10));
          setPage(0);
        }}
      />
    </Box>
  );
};

export default function ManagerList() {
  usePageTitle("Compute Managers");
  const managerQueryBody = useMemo<qcpTypes.ManagerQueryFilters>(
    () => ({
      status: ["active"],
      include: [
        "cluster",
        "compute_tags",
        "name",
        "created_on",
        "modified_on",
        "active_tasks",
        "claimed",
        "successes",
        "failures",
        "rejected",
      ],
    }),
    [],
  );

  const { makeRequest } = usePortalClient();

  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(20);
  const [filter, setFilter] = useState("");

  const {
    status,
    data: managerData,
    error,
  } = useQuery({
    queryKey: ["listManagers"],
    queryFn: () =>
      makeRequest<qcpTypes.Manager[]>(
        "POST",
        "/api/v1/managers/query",
        managerQueryBody,
      ),
  });

  const filteredManagers = useMemo(() => {
    if (!managerData) return [];
    return managerData.filter(
      (m) =>
        m.name.toLowerCase().includes(filter.toLowerCase()) ||
        m.cluster.toLowerCase().includes(filter.toLowerCase()),
    );
  }, [managerData, filter]);

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
          Active Managers
        </Typography>
        {managerData && <ComputeTagSummary managers={managerData} />}
        <Box width={"30%"} mb={2}>
          <TextField
            fullWidth
            variant="outlined"
            size="small"
            label="Filter managers"
            value={filter}
            onChange={handleFilterChange}
          />
        </Box>

        {filteredManagers && filteredManagers.length > 0 ? (
          <>
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell width="50px" />
                    <TableCell width="40%">Name</TableCell>
                    <TableCell width="15%">Cluster</TableCell>
                    <TableCell width="20%">Created / Updated</TableCell>
                    <TableCell width="7%">Claimed</TableCell>
                    <TableCell width="13%">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredManagers
                    .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                    .map((manager) => (
                      <ManagerRow key={manager.name} manager={manager} />
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
            <TablePagination
              rowsPerPageOptions={[10, 20, 50, 100]}
              component="div"
              count={filteredManagers.length}
              rowsPerPage={rowsPerPage}
              page={page}
              onPageChange={handleChangePage}
              onRowsPerPageChange={handleChangeRowsPerPage}
            />
          </>
        ) : (
          <Typography variant="body1">
            {filter ? "(no managers match the filter)" : "(no active managers)"}
          </Typography>
        )}
      </Grid>
    </Grid>
  );
}
