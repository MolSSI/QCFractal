import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import React, { useEffect, useState } from "react";
import { ManagerFragment } from "./ManagerFragment";
import * as qcpTypes from "../PortalTypes";
import { Box, Dialog, DialogContent, Grid, Typography } from "@mui/material";

import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { parseToDate } from "../Utils";

export default function ManagerList() {
  const [managerQueryBody, setManagerQueryBody] =
    useState<qcpTypes.ManagerQueryFilters>({
      status: ["active", "inactive"],
      include: [
        "cluster",
        "name",
        "created_on",
        "modified_on",
        "claimed",
        "successes",
        "failures",
        "rejected",
      ],
      limit: 100,
    });

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
      width: 200,
    },
    {
      field: "successes",
      headerName: "Successes",
      width: 50,
    },
    {
      field: "failures",
      headerName: "Failures",
      width: 50,
    },
    {
      field: "rejected",
      headerName: "Rejected",
      width: 50,
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