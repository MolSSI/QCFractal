import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { usePortalClientRequest } from "../usePortalClient";
import {
  Box,
  Typography,
  Button,
  Chip,
  Paper,
  Grid,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
  Modal,
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
  const [modalOpen, setModalOpen] = useState(false);
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

  const statusColors: Record<
    string,
    | "success"
    | "error"
    | "warning"
    | "default"
    | "info"
    | "primary"
    | "secondary"
  > = {
    complete: "success",
    error: "error",
    waiting: "warning",
    invalid: "default",
    running: "info",
    cancelled: "primary",
    deleted: "secondary",
  };

  return (
    <Paper elevation={3} sx={{ p: 3, mt: 2 }}>
      <Grid container spacing={2}>
        {/* Title and Description */}
        <Grid item xs={12}>
          <Typography variant="h4" fontWeight="bold">
            {recordData.name}
          </Typography>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            {recordData.description}
          </Typography>
        </Grid>
        {/* Status */}
        <Grid item xs={12}>
          <Stack spacing={2}>
            <Button
              variant="outlined"
              onClick={() => setModalOpen(true)}
              sx={{ textTransform: "none" }}
            >
              Manager: {recordData.manager_name}
            </Button>
            <Chip
              label={recordData.status}
              color={statusColors[recordData.status] || "default"}
              sx={{ fontWeight: "bold" }}
            />
          </Stack>
        </Grid>

        {/* Modal for Manager Info */}
        <Modal
          open={modalOpen}
          onClose={() => setModalOpen(false)}
          aria-labelledby="manager-info-title"
          aria-describedby="manager-info-description"
        >
          <Box
            sx={{
              position: "absolute",
              top: "50%",
              left: "50%",
              transform: "translate(-50%, -50%)",
              width: 400,
              bgcolor: "background.paper",
              boxShadow: 24,
              p: 4,
              borderRadius: 2,
            }}
          >
            <Typography id="manager-info-title" variant="h6" fontWeight="bold">
              Manager Information
            </Typography>
            <Typography id="manager-info-description" sx={{ mt: 2 }}>
              Information about manager: {recordData.manager_name}
            </Typography>
          </Box>
        </Modal>

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
