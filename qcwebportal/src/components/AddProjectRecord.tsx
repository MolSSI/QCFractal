import React from "react";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { useDebounce } from "use-debounce";
import { Grid, Stack, TextField, Typography } from "@mui/material";
import { useQuery } from "@tanstack/react-query";

function AddProjectRecord() {
  const { makeRequest } = usePortalClient();

  const [activeManagerParams, setActiveManagerParams] =
    React.useState<qcpTypes.ActiveManagerQuery>({
      programs: {},
      compute_tag: [],
    });
  const [dbActiveManagerParams] = useDebounce(activeManagerParams, 500);

  const hasActiveManagerFilters =
    !!dbActiveManagerParams.programs &&
    Object.keys(dbActiveManagerParams.programs).length > 0 &&
    dbActiveManagerParams.compute_tag.length > 0;

  const {
    data: possibleManagers,
    status: possibleManagersStatus,
  } = useQuery({
    queryKey: ["queryActiveManagers", dbActiveManagerParams],
    queryFn: () =>
      makeRequest<string[]>(
        "POST",
        `/api/v1/managers/queryActive`,
        dbActiveManagerParams,
      ),
    enabled: hasActiveManagerFilters,
  });

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
  };

  return (
    <>
      <Grid
        container
        spacing={2}
        width="100%"
        justifyContent="space-between"
        alignItems="flex-start"
      >
        <Grid size={6}>
          <Typography variant="h5" fontWeight="bold">
            Add Calculation
          </Typography>

          <form onSubmit={handleSubmit}>
            <TextField
              fullWidth
              id="name"
              label="Name"
              variant="outlined"
              margin="normal"
            />
            <TextField
              fullWidth
              id="spec_program"
              label="Program"
              variant="outlined"
              margin="normal"
              onChange={(e) =>
                setActiveManagerParams({
                  ...activeManagerParams,
                  programs: { [e.target.value]: ["unknown"] },
                })
              }
            />

            <Stack>
              <TextField
                fullWidth
                id="compute_tag"
                label="Compute Tag"
                variant="outlined"
                margin="normal"
                onChange={(e) =>
                  e.target.value.length > 0
                    ? setActiveManagerParams({
                        ...activeManagerParams,
                        compute_tag: [e.target.value],
                      })
                    : setActiveManagerParams({
                        ...activeManagerParams,
                        compute_tag: [],
                      })
                }
              />
              <Typography variant="body2">
                {possibleManagersStatus === "pending" && hasActiveManagerFilters
                  ? "Loading active managers..."
                  : possibleManagers
                    ? `${possibleManagers.length} active managers`
                  : "(enter program and compute tag)"}
              </Typography>
            </Stack>
          </form>
        </Grid>
      </Grid>
    </>
  );
}

export default AddProjectRecord;
