import React, { useState } from "react";
import {
  Box,
  Button,
  Chip,
  Collapse,
  IconButton,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";

type DatasetMetadata = {
  dataset_id: number;
  dataset_type: string;
  name: string;
  description?: string;
  tagline?: string;
  tags?: string[];
};

interface DatasetTabProps {
  datasetMetadata: DatasetMetadata[];
  onDelete?: (datasetId: number) => void;
}

export default function DatasetTab({
  datasetMetadata,
  onDelete,
}: DatasetTabProps) {
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const handleToggleExpand = (id: number) => {
    setExpandedId((prev) => (prev === id ? null : id));
  };

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
              {/* Column for dropdown icon */}
              <TableCell />
              <TableCell>
                <strong>Name</strong>
              </TableCell>
              <TableCell>
                <strong>Type</strong>
              </TableCell>
              <TableCell>
                <strong>Content</strong>
              </TableCell>
              <TableCell>
                <strong>Actions</strong>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {datasetMetadata.map((ds) => {
              const isExpanded = expandedId === ds.dataset_id;
              return (
                <React.Fragment key={ds.dataset_id}>
                  {/* Clickable row for toggling the dropdown */}
                  <TableRow
                    hover
                    onClick={() => handleToggleExpand(ds.dataset_id)}
                    sx={{ cursor: "pointer" }}
                  >
                    <TableCell
                      onClick={(e) => {
                        e.stopPropagation();
                        handleToggleExpand(ds.dataset_id);
                      }}
                    >
                      <IconButton size="small">
                        {isExpanded ? (
                          <KeyboardArrowUpIcon />
                        ) : (
                          <KeyboardArrowDownIcon />
                        )}
                      </IconButton>
                    </TableCell>
                    <TableCell>{ds.name}</TableCell>
                    <TableCell>{ds.dataset_type}</TableCell>
                    <TableCell>
                      {/* Placeholder content, adjust based on your dataset info */}
                      100 entries, 2 specifications
                    </TableCell>
                    <TableCell>
                      <Stack direction="row" spacing={1}>
                        <Button
                          variant="contained"
                          size="small"
                          sx={{ mr: 1 }}
                          onClick={(e) => {
                            e.stopPropagation();
                            // Placeholder for view action (new page link later)
                            console.log(
                              "View details for dataset",
                              ds.dataset_id,
                            );
                          }}
                        >
                          View
                        </Button>
                        <Button
                          variant="contained"
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDelete(ds.dataset_id);
                          }}
                          sx={{ ml: 1 }}
                        >
                          Delete
                        </Button>
                      </Stack>
                    </TableCell>
                  </TableRow>

                  {/* Expanded row to show description & tags */}
                  <TableRow>
                    <TableCell
                      style={{ paddingBottom: 0, paddingTop: 0 }}
                      colSpan={5}
                    >
                      <Collapse in={isExpanded} timeout="auto" unmountOnExit>
                        <Box sx={{ margin: 2 }}>
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
                              <Box
                                sx={{
                                  display: "flex",
                                  gap: 1,
                                  flexWrap: "wrap",
                                  mt: 1,
                                }}
                              >
                                {ds.tags.map((tag) => (
                                  <Chip
                                    key={tag}
                                    label={tag}
                                    variant="outlined"
                                  />
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
