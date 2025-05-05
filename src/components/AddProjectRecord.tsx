import React, { useEffect } from "react";
import { useParams } from "react-router-dom";
import { usePortalClientRequest } from "../usePortalClient";
import * as qcpTypes from "../PortalTypes";
import { FetchedData } from "../PortalClientContext";
import { useDebounce } from "use-debounce";
import {
  Typography,
  Chip,
  Grid,
  Stack,
  Paper,
  Box,
  Divider, TextField
} from "@mui/material";
import { format } from "date-fns";


function AddProjectRecord() {
  const { projectId } = useParams();
  const { fetchData } = usePortalClientRequest();

  const [possibleManagers, setPossibleManagers] = React.useState<
    FetchedData<string[]>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  const [activeManagerParams, setActiveManagerParams] = React.useState<qcpTypes.ActiveManagerQuery>({
    programs: {},
    compute_tag: []
   });
   const [dbActiveManagerParams] = useDebounce(activeManagerParams, 500);

  useEffect(() => {
    if (dbActiveManagerParams.programs &&
      Object.keys(dbActiveManagerParams.programs).length > 0 &&
      dbActiveManagerParams.compute_tag.length > 0) {
      fetchData<string[]>(
        setPossibleManagers,
        "post",
        `/api/v1/managers/queryActive`,
        dbActiveManagerParams,
      );
    }
    else {
      setPossibleManagers({
        data: undefined,
        error: undefined,
        loading: true,
      })
    }
  }, [fetchData, dbActiveManagerParams]);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
  }

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
              <Typography variant="h5" fontWeight="bold">Add Calculation</Typography>

              <form
                onSubmit={handleSubmit}
              >
                <TextField fullWidth id="name" label="Name" variant="outlined" margin="normal"/>
                <TextField fullWidth id="spec_program" label="Program" variant="outlined" margin="normal"
                           onChange={(e) => setActiveManagerParams( {...activeManagerParams, programs: {[e.target.value]: ["unknown"]}}) }
                />

                <Stack>
                <TextField fullWidth id="compute_tag" label="Compute Tag" variant="outlined" margin="normal"
                           onChange={(e) =>
                             e.target.value.length > 0 ? setActiveManagerParams({...activeManagerParams, compute_tag: [e.target.value]})
                           :setActiveManagerParams({...activeManagerParams, compute_tag: []})}
                />
                  <Typography variant="body2">{possibleManagers.data ? `${possibleManagers.data?.length} active managers` : "(enter program and compute tag)"}</Typography>
                </Stack>
              </form>
            </Grid>
          </Grid>
    </>
  );
};

export default AddProjectRecord;
