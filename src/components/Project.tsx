// src/pages/Profile.tsx
import { usePortalClientRequest } from "../usePortalClient.ts";
import { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes.ts";
import { useParams } from "react-router-dom";
import {
  Box,
  Button,
  Chip,
  Grid,
  Paper,
  Typography,
} from "@mui/material";

export default function Project() {
    const { projectId } = useParams();

    const { connectionState, makeRequest } = usePortalClientRequest(); // Get client instance here
    const [data, setData] = useState<qcpTypes.Project | undefined>(undefined);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | undefined>(undefined);

    useEffect(() => {
    async function fetchData() {
        setLoading(true);
        const { data, error } = await makeRequest<qcpTypes.Project>(
        "get",
        `api/v1/projects/${projectId}`,
        );
        setData(data);
        setError(error);
        setLoading(false);
        console.log("Project data:", data);
    }
    fetchData();
    }, [projectId, connectionState, makeRequest]);


return (
  <>
    {loading && <Typography>Loading...</Typography>}
    {error && <Typography color="error">{error}</Typography>}

    {data && (
      <Grid container spacing={2}>
        {/* Project name & tagline */}
        <Grid item xs={12}>
          <Typography variant="h4" fontWeight="bold">
            {data.name}
          </Typography>
          <Typography variant="subtitle1" sx={{ color: "text.secondary" }}>
            {data.tagline}
          </Typography>
        </Grid>

        {/* Summary stats: for example, Records, Datasets, Molecules */}
        <Grid item xs={12} sm={4}>
          <Paper elevation={3}>
            <Box p={2}>
              <Typography variant="h6" fontWeight="bold">
                25 Records
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Example count
              </Typography>
            </Box>
          </Paper>
        </Grid>
        <Grid item xs={12} sm={4}>
          <Paper elevation={3}>
            <Box p={2}>
              <Typography variant="h6" fontWeight="bold">
                5 Datasets
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Example count
              </Typography>
            </Box>
          </Paper>
        </Grid>
        <Grid item xs={12} sm={4}>
          <Paper elevation={3}>
            <Box p={2}>
              <Typography variant="h6" fontWeight="bold">
                25 Molecules
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Example count
              </Typography>
            </Box>
          </Paper>
        </Grid>

        {/* Description & Metadata section */}
        <Grid item xs={12}>
          <Paper elevation={2}>
            <Box display="flex" alignItems="center" justifyContent="left" p={2}>
              <Typography variant="h6" fontWeight="bold">
                Description & Metadata
              </Typography>
              <Button sx={{ ml: 2 }} variant="outlined" color="primary">
                Edit
              </Button>
            </Box>
            <Box p={2}>
              <Typography variant="h6" fontWeight="bold" gutterBottom>
                Description
              </Typography>
              <Typography variant="body1" paragraph>
                {data.description}
              </Typography>

              <Typography variant="h6" fontWeight="bold" gutterBottom>
                Tags
              </Typography>
              <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
                {data.tags?.map((tag, idx) => (
                  <Chip key={idx} label={tag} variant="outlined" />
                ))}
              </Box>

              <Box mt={2}>
                <Typography variant="h6" fontWeight="bold" gutterBottom>
                  Owner
                </Typography>
                <Typography variant="body1">
                  {data.owner_user || "N/A"}
                </Typography>
              </Box>
            </Box>
          </Paper>
        </Grid>
      </Grid>
    )}
  </>
);
}
