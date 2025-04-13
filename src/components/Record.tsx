import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { usePortalClientRequest } from "../usePortalClient";
import {
  Box,
  Typography,
  Chip,
  Paper,
  Grid,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
} from "@mui/material";

interface RecordData {
  record_type: string;
  manager_name: string;
  status: string;
  owner_user: string | null;
  owner_group: string | null;
  id: number;
  specification: {
    driver: string;
    basis: string;
    method: string;
    program: string;
    keywords: Record<string, any>;
  };
  properties: Record<string, any>;
}

const Record: React.FC = () => {
  const { projectId, recordId } = useParams();
  const { makeRequest } = usePortalClientRequest();
  const [recordData, setRecordData] = useState<RecordData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchRecord() {
      setLoading(true);
      const { data, error } = await makeRequest<RecordData>(
        "get",
        `/api/v1/projects/${projectId}/records/${recordId}`,
      );
      setRecordData(data ?? null);
      setError(error);
      setLoading(false);
    }
    fetchRecord();
  }, [projectId, recordId, makeRequest]);

  if (loading) {
    return <Typography>Loading...</Typography>;
  }

  if (error) {
    return <Typography color="error">{error}</Typography>;
  }

  if (!recordData) {
    return <Typography>No record data available.</Typography>;
  }

  return (
    <Paper elevation={3} sx={{ p: 3, mt: 2 }}>
      <Grid container spacing={2}>
        {/* Title and Description */}
        <Grid item xs={12}>
          <Typography variant="h4" fontWeight="bold">
            Record {recordData.id}
          </Typography>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            This record was created using the {recordData.specification.driver}{" "}
            driver with the {recordData.specification.method} method and the{" "}
            {recordData.specification.basis} basis set.
          </Typography>
        </Grid>
        {/* Status */}
        <Grid item xs={12}>
          <Stack spacing={2}>
            <Chip
              label={recordData.status}
              color={
                recordData.status === "complete"
                  ? "success"
                  : recordData.status === "error"
                    ? "error"
                    : "default"
              }
              sx={{ fontWeight: "bold" }}
            />
          </Stack>
        </Grid>

        {/* Specification and Properties Tables */}
        {/* Specification and Properties Tables */}
        <Box sx={{ width: "100%", mx: "auto" }}>
          <Grid item xs={12} container spacing={2}>
            {/* Specification Table */}
            <Grid item xs={12} md={6}>
              <Typography variant="h6" fontWeight="bold" gutterBottom>
                Specification
              </Typography>
              <TableContainer component={Paper}>
                <Table size="small">
                  <TableBody>
                    {Object.entries(recordData.specification ?? {}) // Use an empty object if specification is null or undefined
                      .filter(([key]) => key !== "keywords") // Exclude keywords from the main specification table
                      .filter(([_, value]) => value !== null) // Exclude null values
                      .map(([key, value]) => (
                        <TableRow key={key}>
                          <TableCell>{key}</TableCell>
                          <TableCell>
                            {typeof value === "object" &&
                            Object.keys(value).length === 0
                              ? "N/A" // Replace empty objects with "N/A"
                              : value.toString()}
                          </TableCell>
                        </TableRow>
                      ))}
                    {Object.entries(recordData.specification.keywords)
                      .filter(([_, value]) => value !== null) // Exclude null values
                      .map(([key, value]) => (
                        <TableRow key={key}>
                          <TableCell>{key}</TableCell>
                          <TableCell>
                            {typeof value === "object" &&
                            Object.keys(value).length === 0
                              ? "N/A" // Replace empty objects with "N/A"
                              : value.toString()}
                          </TableCell>
                        </TableRow>
                      ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Grid>

            {/* Properties Table */}
            <Grid item xs={12} md={6}>
              <Typography variant="h6" fontWeight="bold" gutterBottom>
                Properties
              </Typography>
              <TableContainer component={Paper}>
                <Table size="small">
                  <TableBody>
                    {Object.entries(recordData.properties ?? {}) // Use an empty object if properties is null or undefined
                      .filter(([_, value]) => value !== null) // Exclude null values
                      .map(([key, value]) => (
                        <TableRow key={key}>
                          <TableCell>{key}</TableCell>
                          <TableCell>
                            {typeof value === "object" &&
                            Object.keys(value).length === 0
                              ? "N/A" // Replace empty objects with "N/A"
                              : value.toString()}
                          </TableCell>
                        </TableRow>
                      ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Grid>
          </Grid>
        </Box>
      </Grid>
    </Paper>
  );
};

export default Record;
