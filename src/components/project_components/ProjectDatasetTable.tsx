import React, { useState } from "react";
import { Link } from "react-router-dom";
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
  TablePagination,
  TableRow,
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import * as qcpTypes from "../../PortalTypes";
import { RecordType } from "../RecordType";

interface ProjectDatasetTableProps {
  datasetMetadata: qcpTypes.ProjectDatasetMetadata[];
  onDelete?: (datasetId: number) => void;
}

function DatasetRow({
  ds,
  onDelete,
}: {
  ds: qcpTypes.ProjectDatasetMetadata;
  onDelete?: (datasetId: number) => void;
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  const handleToggleExpand = () => {
    setIsExpanded((prev) => !prev);
  };

  const handleDelete = (id: number) => {
    if (onDelete) {
      onDelete(id);
    } else {
      console.log("Delete dataset with ID:", id);
    }
  };

  return (
    <React.Fragment>
      {/* Clickable row for toggling the dropdown */}
      <TableRow hover onClick={handleToggleExpand} sx={{ cursor: "pointer" }}>
        <TableCell width="50px">
          <IconButton size="small">
            {isExpanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell>
          <Box>
            <Typography fontSize={"1.0rem"} fontWeight={"bold"}>
              [{ds.dataset_id}] {ds.name}
            </Typography>
            <Typography>
              {ds.tagline}
            </Typography>
          </Box>
        </TableCell>
        <TableCell><RecordType type={ds.dataset_type} /></TableCell>
        <TableCell>
          <Stack direction="row" spacing={1}>
            <Button
              variant="contained"
              size="small"
              component={Link}
              to={`/datasets/${ds.dataset_id}`}
              onClick={(e) => {
                e.stopPropagation();
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
            >
              Delete
            </Button>
          </Stack>
        </TableCell>
      </TableRow>

      {/* Expanded row to show description & tags */}
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={5}>
          <Collapse in={isExpanded} timeout="auto" unmountOnExit>
            <Box m={3} display="flex" flexDirection="column" gap={2}>
              {ds.description && (
                <Box>
                  <Typography variant="body2" fontWeight="bold">
                    Description
                  </Typography>
                  <Typography variant="body2">{ds.description}</Typography>
                </Box>
              )}
              {ds.tags && ds.tags.length > 0 && (
                <Box>
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
                      <Chip key={tag} label={tag} variant="outlined" />
                    ))}
                  </Box>
                </Box>
              )}
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
}

export default function ProjectDatasetTable({
  datasetMetadata,
  onDelete,
}: ProjectDatasetTableProps) {
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  return (
    <Box>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small" aria-label="dataset table">
          <TableHead>
            <TableRow>
              <TableCell width="50px" />
              <TableCell>
                <strong>Name</strong>
              </TableCell>
              <TableCell>
                <strong>Type</strong>
              </TableCell>
              <TableCell>
                <strong>Actions</strong>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {datasetMetadata
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((ds) => (
                <DatasetRow
                  key={ds.dataset_id}
                  ds={ds}
                  onDelete={onDelete}
                />
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 25, 50]}
        component="div"
        count={datasetMetadata.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </Box>
  );
}
