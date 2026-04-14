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
  TextField,
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import * as qcpTypes from "../../PortalTypes";
import { RecordTypeChip } from "../RecordTypeChip.tsx";
import ReactMarkdown from "react-markdown";
import { LinkDatasetButton } from "./LinkDatasetDialog";
import { UnlinkDatasetButton } from "./UnlinkDatasetDialog";
import { useAuth } from "../../Auth.tsx";

interface ProjectDatasetTableProps {
  projectId: number;
  datasetMetadata: qcpTypes.ProjectDatasetMetadata[];
  onRefresh?: () => void;
}

function DatasetRow({
  projectId,
  ds,
  onRefresh,
  canModify,
}: {
  projectId: number;
  ds: qcpTypes.ProjectDatasetMetadata;
  onRefresh?: () => void;
  canModify: boolean;
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  const handleToggleExpand = () => {
    setIsExpanded((prev) => !prev);
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
            <Typography>{ds.tagline}</Typography>
          </Box>
        </TableCell>
        <TableCell>
          <RecordTypeChip type={ds.dataset_type} />
        </TableCell>
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
            {canModify && (
              <UnlinkDatasetButton
                projectId={projectId}
                dataset={ds}
                onRefresh={onRefresh}
              />
            )}
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
                  <Typography variant="body2">
                    <ReactMarkdown>{ds.description.trim()}</ReactMarkdown>
                  </Typography>
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
  projectId,
  datasetMetadata,
  onRefresh,
}: ProjectDatasetTableProps) {
  const { has_permission } = useAuth();

  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [filter, setFilter] = useState("");

  const canModify = has_permission("projects", "modify");

  const filteredDatasets = React.useMemo(() => {
    return datasetMetadata.filter((ds) =>
      ds.name.toLowerCase().includes(filter.toLowerCase()),
    );
  }, [datasetMetadata, filter]);

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleFilterChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setFilter(event.target.value);
    setPage(0);
  };

  return (
    <Box>
      <Stack
        direction="row"
        spacing={2}
        justifyContent="space-between"
        alignItems="center"
        sx={{ mb: 2 }}
      >
        <Box width={"30%"}>
          <TextField
            fullWidth
            variant="outlined"
            size="small"
            label="Filter datasets"
            value={filter}
            onChange={handleFilterChange}
          />
        </Box>
        {canModify && (
          <LinkDatasetButton projectId={projectId} onRefresh={onRefresh} />
        )}
      </Stack>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small" aria-label="dataset table">
          <TableHead>
            <TableRow>
              <TableCell width="50px" />
              <TableCell>Name</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filteredDatasets
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((ds) => (
                <DatasetRow
                  key={ds.dataset_id}
                  projectId={projectId}
                  ds={ds}
                  onRefresh={onRefresh}
                  canModify={canModify}
                />
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 25, 50]}
        component="div"
        count={filteredDatasets.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </Box>
  );
}
