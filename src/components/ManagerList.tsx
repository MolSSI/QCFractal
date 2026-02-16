import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import { useEffect, useState, useMemo } from "react";
import { ManagerFragment } from "./ManagerFragment";
import * as qcpTypes from "../PortalTypes";
import {
  Box,
  Stack,
  Dialog,
  DialogContent,
  Grid,
  Typography,
} from "@mui/material";

import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { parseToDate } from "../Utils";
import { PieChart } from "@mui/x-charts/PieChart";

export default function ManagerList() {
  const managerQueryBody = useMemo<qcpTypes.ManagerQueryFilters>(
    () => ({
      status: ["active", "inactive"],
      include: [
        "cluster",
        "name",
        "created_on",
        "modified_on",
        "active_tasks",
        "claimed",
        "successes",
        "failures",
        "rejected",
      ],
      limit: 100,
    }),
    [],
  );

  const { fetchData } = usePortalClientRequest(); // Get client instance here

  const [managerFetchedData, setmanagerFetchedData] = useState<
    FetchedData<Array<qcpTypes.Manager>>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  // Load project data, then dataset & record metadata
  useEffect(() => {
    fetchData<Array<qcpTypes.Manager>>(
      setmanagerFetchedData,
      "post",
      `api/v1/managers/query`,
      managerQueryBody,
    );
  }, [fetchData, managerQueryBody]);

  const managerData = managerFetchedData?.data;

  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedManagerName, setSelectedManagerName] = useState("");

  const columns: GridColDef<qcpTypes.Manager>[] = [
    {
      field: "name",
      headerName: "Name",
      minWidth: 470,
      renderCell: (params) => {
        return (
          <Box
            sx={{ cursor: "pointer" }}
            onClick={() => {
              setSelectedManagerName(params.row.name);
              setDialogOpen(true);
            }}
          >
            {params.row.name}
          </Box>
        );
      },
    },
    {
      field: "cluster",
      headerName: "Cluster",
      width: 150,
    },
    {
      field: "created_on",
      headerName: "Created On",
      width: 250,
      valueGetter: (value) => {
        return parseToDate(value)?.toLocaleString();
      },
    },
    {
      field: "modified_on",
      headerName: "Last updated",
      width: 250,
      valueGetter: (value) => {
        return parseToDate(value)?.toLocaleString();
      },
    },
    {
      field: "claimed",
      headerName: "Claimed",
      width: 300,
      renderCell: (params) => {
        const returned =
          params.row.claimed -
          (params.row.active_tasks +
            params.row.successes +
            params.row.failures);
        return (
          <Stack direction="row" spacing={1}>
            <Typography variant="body1">{params.row.claimed}</Typography>
            <PieChart
              skipAnimation={true}
              width={40}
              height={40}
              hideLegend={true}
              colors={["blue", "green", "red", "orange"]}
              series={[
                {
                  data: [
                    { id: 0, value: params.row.active_tasks, label: "active" },
                    { id: 1, value: params.row.successes, label: "success" },
                    { id: 2, value: params.row.failures, label: "failed" },
                    { id: 3, value: returned, label: "returned" },
                  ],
                },
              ]}
            />
          </Stack>
        );
      },
    },
  ];

  return (
    <>
      {managerFetchedData.loading && <Typography>Loading...</Typography>}

      <Grid container spacing={2} width="100%">
        <Grid size={12}>
          <Typography variant="h4" marginBottom={3}>
            Active Managers
          </Typography>

          {managerData && managerData.length > 0 ? (
            <>
              <Dialog
                open={dialogOpen}
                onClose={() => {
                  setDialogOpen(false);
                }}
              >
                {dialogOpen && (
                  <>
                    <DialogContent>
                      <ManagerFragment managerName={selectedManagerName} />
                    </DialogContent>
                  </>
                )}
              </Dialog>

              <DataGrid
                rows={managerData}
                columns={columns}
                initialState={{
                  pagination: {
                    paginationModel: {
                      pageSize: 10,
                    },
                  },
                }}
                pageSizeOptions={[10, 25, 100]}
              />
            </>
          ) : (
            <Typography variant="body1">(no active managers)</Typography>
          )}
        </Grid>
      </Grid>
    </>
  );
}
