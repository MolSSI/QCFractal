import React, { useState } from "react";
import {
  Box,
  Button,
  Chip,
  Collapse,
  IconButton,
  Paper,
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
import { useNavigate, useParams } from "react-router-dom";

type RecordMetadata = {
  record_id: number;
  name: string;
  status: string;
  description?: string;
  tags?: string[];
};

interface RecordTabProps {
  recordMetadata: RecordMetadata[];
  onDelete?: (recordId: number) => void;
}

export default function RecordTab({
  recordMetadata,
  onDelete,
}: RecordTabProps) {
    const navigate = useNavigate();
    const { projectId } = useParams();
    const [expandedId, setExpandedId] = useState<number | null>(null);

    const handleToggleExpand = (id: number) => {
    setExpandedId((prev) => (prev === id ? null : id));
    };

    const handleViewClick = (recordId: number) => {
    navigate(`/projects/${projectId}/records/${recordId}`);
    };

    const handleDelete = (id: number) => {
    if (onDelete) {
        onDelete(id);
    } else {
        console.log("Delete record with ID:", id);
    }
    };

  return (
    <Box>
      <TableContainer component={Paper}>
        <Table size="small" aria-label="record table">
          <TableHead>
            <TableRow>
              {/* Dropdown arrow column */}
              <TableCell />
              <TableCell>
                <strong>Name</strong>
              </TableCell>
              <TableCell>
                <strong>Status</strong>
              </TableCell>
              <TableCell>
                <strong>Actions</strong>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {recordMetadata.map((record) => {
              const isExpanded = expandedId === record.record_id;
              return (
                <React.Fragment key={record.record_id}>
                  {/* Main record row */}
                  <TableRow
                    hover
                    onClick={() => handleToggleExpand(record.record_id)}
                    sx={{ cursor: "pointer" }}
                  >
                    <TableCell
                      onClick={(e) => {
                        e.stopPropagation();
                        handleToggleExpand(record.record_id);
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
                    <TableCell>{record.name}</TableCell>
                    <TableCell>{record.status}</TableCell>
                    <TableCell>
                      <Button
                        variant="contained"
                        size="small"
                        onClick={() => handleViewClick(record.record_id)}
                      >
                        View
                      </Button>
                      <Button
                        variant="outlined"
                        color="error"
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleDelete(record.record_id);
                        }}
                        sx={{ ml: 1 }}
                      >
                        Delete
                      </Button>
                    </TableCell>
                  </TableRow>

                  {/* Expanded row with description & tags */}
                  <TableRow>
                    <TableCell
                      style={{ paddingBottom: 0, paddingTop: 0 }}
                      colSpan={4}
                    >
                      <Collapse in={isExpanded} timeout="auto" unmountOnExit>
                        <Box sx={{ m: 2 }}>
                          {record.description && (
                            <>
                              <Typography variant="body2" fontWeight="bold">
                                Description
                              </Typography>
                              <Typography variant="body2" paragraph>
                                {record.description}
                              </Typography>
                            </>
                          )}
                          {record.tags && record.tags.length > 0 && (
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
                                {record.tags.map((tag) => (
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
