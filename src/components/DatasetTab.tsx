import React, { useState } from "react";
import {
  Box,
  Button,
  Chip,
  Collapse,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography
} from "@mui/material";

type DatasetMetadata = {
  dataset_id: number;
  dataset_type: string;
  name: string;
  description?: string;
  tagline?: string;
  tags?: string[];
  // ...any other fields from your API
};

interface DatasetTabProps {
  datasetMetadata: DatasetMetadata[];
  onDelete?: (datasetId: number) => void;
}

export default function DatasetTab({ datasetMetadata, onDelete }: DatasetTabProps) {
  // Track which dataset row is expanded
  const [expandedId, setExpandedId] = useState<number | null>(null);

  // Simple toggle for row expansion
  const handleToggleExpand = (id: number) => {
    setExpandedId((prev) => (prev === id ? null : id));
  };

  // Placeholder for the Delete action
  const handleDelete = (id: number) => {
    if (onDelete) {
      onDelete(id);
    } else {
      console.log("Delete dataset with ID:", id);
    }
  };

  return (
    <Box>
      <TableContainer component={Paper}>
        <Table size="small" aria-label="dataset table">
          <TableHead>
            <TableRow>
              <TableCell><strong>Name</strong></TableCell>
              <TableCell><strong>Type</strong></TableCell>
              <TableCell><strong>Content</strong></TableCell>
              <TableCell><strong>Actions</strong></TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {datasetMetadata.map((ds) => {
              const isExpanded = expandedId === ds.dataset_id;
              return (
                <React.Fragment key={ds.dataset_id}>
                  {/* Main row */}
                  <TableRow>
                    <TableCell>{ds.name}</TableCell>
                    <TableCell>{ds.dataset_type}</TableCell>
                    {/* 'Content' placeholder: adapt to your data */}
                    <TableCell>
                      {/* 
                        Here you can dynamically build something like:
                        "100 entries, 2 specifications, 200 records"
                        if you have that info in the dataset object.
                      */}
                      100 entries, 2 specifications
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="contained"
                        size="small"
                        onClick={() => handleToggleExpand(ds.dataset_id)}
                      >
                        {isExpanded ? "Hide" : "View"}
                      </Button>
                      <Button
                        variant="contained"
                        size="small"
                        onClick={() => handleDelete(ds.dataset_id)}
                        sx={{ ml: 1 }}
                      >
                        Delete
                      </Button>
                    </TableCell>
                  </TableRow>

                  {/* Expanded row showing description & tags */}
                  <TableRow>
                    <TableCell
                      style={{ paddingBottom: 0, paddingTop: 0 }}
                      colSpan={4}
                    >
                      <Collapse in={isExpanded} timeout="auto" unmountOnExit>
                        <Box sx={{ m: 2 }}>
                          {ds.description && (
                            <>
                              <Typography variant="body2" fontWeight="bold">
                                Description
                              </Typography>
                              <Typography variant="body2" paragraph>
                                {ds.description}
                              </Typography>
                            </>
                          )}
                          {ds.tags && ds.tags.length > 0 && (
                            <>
                              <Typography variant="body2" fontWeight="bold">
                                Tags
                              </Typography>
                              <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap", mt: 1 }}>
                                {ds.tags.map((tag) => (
                                  <Chip key={tag} label={tag} variant="outlined" />
                                ))}
                              </Box>
                            </>
                          )}
                        </Box>
                      </Collapse>
                    </TableCell>
                  </TableRow>
                </React.Fragment>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
