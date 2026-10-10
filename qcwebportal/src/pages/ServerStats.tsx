import { useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { LineChart } from "@mui/x-charts/LineChart";
import DownloadIcon from "@mui/icons-material/Download";
import {
  Box,
  Button,
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
} from "@mui/material";
import { usePortalClient } from "../PortalClient.tsx";
import { usePageTitle } from "../UsePageTitle.ts";
import * as qcpTypes from "../PortalTypes.ts";
import LoadingIndicator from "../components/LoadingIndicator.tsx";
import ErrorIndicator from "../components/ErrorIndicator.tsx";
import {
  dateStringToLocalTime,
  formatCpuHoursSpan,
  formatSize,
} from "../Utils.ts";

type PlotMode = "cumulative" | "daily";

type ChartRow = {
  date: Date;
  dailyRecordCount: number;
  cumulativeRecordCount: number;
  dailyCpuHours: number;
  cumulativeCpuHours: number;
};

type SummaryRow = {
  label: string;
  value: ReactNode;
};

type BreakdownRow = {
  recordType: string;
  statuses: Record<string, number>;
  total: number;
};

const preferredStatusOrder: qcpTypes.RecordStatus[] = [
  "complete",
  "running",
  "waiting",
  "error",
  "cancelled",
  "invalid",
  "deleted",
];

function formatInteger(value: number): string {
  return value.toLocaleString();
}

function formatCpuHours(value: number): string {
  return value.toLocaleString(undefined, {
    maximumFractionDigits: value >= 100 ? 0 : 2,
  });
}

function formatCompact(value: number): string {
  return new Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function downloadStats(data: qcpTypes.ServerStatsEntry[]) {
  const blob = new Blob([JSON.stringify(data, null, 2)], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "server_stats.json";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export default function ServerStats() {
  usePageTitle("Server Statistics");
  const { makeRequest } = usePortalClient();
  const [plotMode, setPlotMode] = useState<PlotMode>("cumulative");

  const {
    status,
    data: serverStats,
    error,
  } = useQuery({
    queryKey: ["serverStats"],
    queryFn: () =>
      makeRequest<qcpTypes.ServerStatsEntry[]>("GET", "/api/v1/server_stats"),
  });

  const sortedStats = useMemo(() => {
    if (!serverStats) {
      return [];
    }

    return [...serverStats].sort((a, b) => a.date.localeCompare(b.date));
  }, [serverStats]);

  const chartData = useMemo<ChartRow[]>(() => {
    return sortedStats.reduce<{
      rows: ChartRow[];
      cumulativeRecordCount: number;
      cumulativeCpuHours: number;
    }>(
      (acc, entry) => {
        const cumulativeRecordCount =
          acc.cumulativeRecordCount + entry.record_count;
        const cumulativeCpuHours = acc.cumulativeCpuHours + entry.cpu_hours;

        return {
          cumulativeRecordCount,
          cumulativeCpuHours,
          rows: [
            ...acc.rows,
            {
              date: new Date(`${entry.date}T00:00:00`),
              dailyRecordCount: entry.record_count,
              cumulativeRecordCount,
              dailyCpuHours: entry.cpu_hours,
              cumulativeCpuHours,
            },
          ],
        };
      },
      { rows: [], cumulativeRecordCount: 0, cumulativeCpuHours: 0 },
    ).rows;
  }, [sortedStats]);

  const latestEntry =
    sortedStats.length > 0 ? sortedStats[sortedStats.length - 1] : undefined;

  const currentDetails = useMemo<qcpTypes.ServerStatsRecordCountDetails>(
    () => latestEntry?.record_count_details ?? {},
    [latestEntry],
  );

  const breakdownStatuses = useMemo(() => {
    const discoveredStatuses = new Set<string>();

    Object.values(currentDetails).forEach((statuses) => {
      Object.keys(statuses ?? {}).forEach((statusName) => {
        discoveredStatuses.add(statusName);
      });
    });

    return [...discoveredStatuses].sort((a, b) => {
      const preferredA = preferredStatusOrder.indexOf(
        a as qcpTypes.RecordStatus,
      );
      const preferredB = preferredStatusOrder.indexOf(
        b as qcpTypes.RecordStatus,
      );

      if (preferredA === -1 && preferredB === -1) {
        return a.localeCompare(b);
      }
      if (preferredA === -1) {
        return 1;
      }
      if (preferredB === -1) {
        return -1;
      }

      return preferredA - preferredB;
    });
  }, [currentDetails]);

  const breakdownRows = useMemo<BreakdownRow[]>(() => {
    return Object.entries(currentDetails)
      .sort(([typeA], [typeB]) => typeA.localeCompare(typeB))
      .map(([recordType, statuses]) => {
        const total = breakdownStatuses.reduce(
          (acc, statusName) =>
            acc + (statuses[statusName as qcpTypes.RecordStatus] ?? 0),
          0,
        );

        return {
          recordType,
          statuses: Object.fromEntries(
            breakdownStatuses.map((statusName) => [
              statusName,
              statuses[statusName as qcpTypes.RecordStatus] ?? 0,
            ]),
          ),
          total,
        };
      });
  }, [breakdownStatuses, currentDetails]);

  const breakdownTotals = useMemo(() => {
    const totals = Object.fromEntries(
      breakdownStatuses.map((statusName) => [statusName, 0]),
    ) as Record<string, number>;

    for (const row of breakdownRows) {
      for (const statusName of breakdownStatuses) {
        totals[statusName] += row.statuses[statusName] ?? 0;
      }
    }

    return totals;
  }, [breakdownRows, breakdownStatuses]);

  const totalCpuHours = useMemo(
    () => sortedStats.reduce((acc, entry) => acc + entry.cpu_hours, 0),
    [sortedStats],
  );

  const summaryRows = useMemo<SummaryRow[]>(() => {
    if (!latestEntry) {
      return [];
    }

    const cpuHoursSpan = formatCpuHoursSpan(totalCpuHours);

    return [
      {
        label: "Estimated CPU hours",
        value: (
          <>
            {formatCpuHours(totalCpuHours)}
            {cpuHoursSpan && (
              <Box
                component="span"
                sx={{ ml: 1.5, color: "text.secondary" }}
              >
                ({cpuHoursSpan})
              </Box>
            )}
          </>
        ),
      },
      {
        label: "Database size",
        value: formatSize(latestEntry.database_size),
      },
    ];
  }, [latestEntry, totalCpuHours]);

  const recordSeriesKey =
    plotMode === "cumulative" ? "cumulativeRecordCount" : "dailyRecordCount";
  const cpuSeriesKey =
    plotMode === "cumulative" ? "cumulativeCpuHours" : "dailyCpuHours";
  const plotLabelPrefix = plotMode === "cumulative" ? "Cumulative" : "Daily";

  if (status === "pending") {
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

  if (status === "error") {
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
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: { xs: "flex-start", sm: "center" },
            gap: 2,
            flexDirection: { xs: "column", sm: "row" },
            mb: 1,
          }}
        >
          <Typography variant="h4">Server Statistics</Typography>
          <Button
            variant="outlined"
            startIcon={<DownloadIcon />}
            onClick={() => downloadStats(serverStats ?? [])}
            disabled={!serverStats || serverStats.length === 0}
          >
            Download JSON
          </Button>
        </Box>
      </Grid>

      <Grid size={12}>
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Box
            sx={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: { xs: "flex-start", md: "center" },
              gap: 2,
              flexDirection: { xs: "column", md: "row" },
              mb: 1,
            }}
          >
            <Typography variant="h6" fontWeight="bold">
              Records and CPU Estimate
            </Typography>
            <ToggleButtonGroup
              size="small"
              exclusive
              value={plotMode}
              onChange={(_event, value: PlotMode | null) => {
                if (value) {
                  setPlotMode(value);
                }
              }}
              aria-label="plot mode"
            >
              <ToggleButton value="cumulative">Cumulative</ToggleButton>
              <ToggleButton value="daily">Daily</ToggleButton>
            </ToggleButtonGroup>
          </Box>

          {chartData.length > 0 ? (
            <Box sx={{ width: "100%", height: 380 }}>
              <LineChart
                xAxis={[
                  {
                    data: chartData.map((entry) => entry.date),
                    scaleType: "time",
                    label: "Date",
                    height: 50,
                    valueFormatter: (value: Date) =>
                      value.toLocaleDateString(undefined, {
                        month: "short",
                        day: "numeric",
                        year: "numeric",
                      }),
                  },
                ]}
                yAxis={[
                  {
                    id: "records",
                    label: `${plotLabelPrefix} Records`,
                    width: 90,
                    valueFormatter: (value: number) => formatCompact(value),
                  },
                  {
                    id: "cpuHours",
                    label: `${plotLabelPrefix} CPU Hours`,
                    position: "right",
                    width: 96,
                    valueFormatter: (value: number) => formatCompact(value),
                  },
                ]}
                series={[
                  {
                    id: "recordCount",
                    yAxisId: "records",
                    data: chartData.map((entry) => entry[recordSeriesKey]),
                    label: `${plotLabelPrefix} record count`,
                    showMark: false,
                    valueFormatter: (value: number | null) =>
                      value === null ? "N/A" : formatInteger(value),
                  },
                  {
                    id: "cpuHours",
                    yAxisId: "cpuHours",
                    data: chartData.map((entry) => entry[cpuSeriesKey]),
                    label: `${plotLabelPrefix} CPU hours`,
                    showMark: false,
                    valueFormatter: (value: number | null) =>
                      value === null ? "N/A" : formatCpuHours(value),
                  },
                ]}
                grid={{ horizontal: true }}
                height={340}
                margin={{ left: 5, right: 5, top: 20, bottom: 20 }}
              />
            </Box>
          ) : (
            <Box
              sx={{
                minHeight: 320,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: "text.secondary",
              }}
            >
              <Typography>No server statistics available.</Typography>
            </Box>
          )}
        </Paper>
      </Grid>

      <Grid size={12}>
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
            Current Data
          </Typography>
          <Typography variant="body1" sx={{ mb: 2 }}>
            Last updated: {dateStringToLocalTime(latestEntry?.timestamp)}
          </Typography>

          {latestEntry ? (
            <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
              <TableContainer component={Paper} variant="outlined">
                <Table size="small">
                  <TableBody>
                    {summaryRows.map((row) => (
                      <TableRow key={row.label}>
                        <TableCell sx={{ fontWeight: 600, width: "32%" }}>
                          {row.label}
                        </TableCell>
                        <TableCell>{row.value}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>

              {breakdownStatuses.length > 0 && breakdownRows.length > 0 ? (
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ fontWeight: 700 }}>
                          Record Type
                        </TableCell>
                        {breakdownStatuses.map((statusName) => (
                          <TableCell
                            key={statusName}
                            align="right"
                            sx={{
                              fontWeight: 700,
                              textTransform: "capitalize",
                            }}
                          >
                            {statusName}
                          </TableCell>
                        ))}
                        <TableCell align="right" sx={{ fontWeight: 700 }}>
                          Total
                        </TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {breakdownRows.map((row) => (
                        <TableRow key={row.recordType}>
                          <TableCell sx={{ textTransform: "capitalize" }}>
                            {row.recordType}
                          </TableCell>
                          {breakdownStatuses.map((statusName) => (
                            <TableCell key={statusName} align="right">
                              {row.statuses[statusName] > 0
                                ? formatInteger(row.statuses[statusName])
                                : "-"}
                            </TableCell>
                          ))}
                          <TableCell align="right" sx={{ fontWeight: 600 }}>
                            {formatInteger(row.total)}
                          </TableCell>
                        </TableRow>
                      ))}
                      <TableRow>
                        <TableCell sx={{ fontWeight: 700 }}>
                          All types
                        </TableCell>
                        {breakdownStatuses.map((statusName) => (
                          <TableCell
                            key={statusName}
                            align="right"
                            sx={{ fontWeight: 700 }}
                          >
                            {breakdownTotals[statusName] > 0
                              ? formatInteger(breakdownTotals[statusName])
                              : "-"}
                          </TableCell>
                        ))}
                        <TableCell align="right" sx={{ fontWeight: 700 }}>
                          {formatInteger(
                            breakdownRows.reduce(
                              (acc, row) => acc + row.total,
                              0,
                            ),
                          )}
                        </TableCell>
                      </TableRow>
                    </TableBody>
                  </Table>
                </TableContainer>
              ) : (
                <Typography color="text.secondary">
                  No record-type breakdown available.
                </Typography>
              )}
            </Box>
          ) : (
            <Box
              sx={{
                minHeight: 320,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: "text.secondary",
              }}
            >
              <Typography>No server statistics available.</Typography>
            </Box>
          )}
        </Paper>
      </Grid>

      <Grid size={12}>
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
            Notes
          </Typography>
          <Typography component={"p"} sx={{mb: 2}}>
            CPU time is an estimate based on the number of cores multiplied by the walltime, which is not completely accurate.
            This also risks double counting CPU time if reported by both parent records (for example, optimizations) and child records (trajectories).
          </Typography>
          <Typography component={"p"}>
            Record counts and CPU  time may also be inaccurate for instances with lots of churn related to deleting old records.
          </Typography>
        </Paper>
      </Grid>
    </Grid>
  );
}
