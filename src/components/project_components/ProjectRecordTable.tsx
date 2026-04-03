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
  TablePagination,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import { useNavigate, useParams } from "react-router-dom";
import * as qcpTypes from "../../PortalTypes";
import { RecordTypeChip } from "../RecordTypeChip.tsx";
import { StatusChip } from "../StatusChip.tsx";

interface ProjectRecordTableProps {
  recordMetadata: qcpTypes.ProjectRecordMetadata[];
  onDelete?: (recordId: number) => void;
}

function RecordRow({
  record,
  onDelete,
  projectId,
}: {
  record: qcpTypes.ProjectRecordMetadata;
  onDelete?: (recordId: number) => void;
  projectId?: string;
}) {
  const navigate = useNavigate();
  const [isExpanded, setIsExpanded] = useState(false);

  const handleToggleExpand = () => {
    setIsExpanded((prev) => !prev);
  };

  const handleViewClick = (recordId: number) => {
    navigate(`/projects/${projectId}/records/${recordId}`, {
      state: { activeTab: 1 },
    });
  };

  const handleDelete = (id: number) => {
    if (onDelete) {
      onDelete(id);
    } else {
      console.log("Delete record with ID:", id);
    }
  };

  return (
    <React.Fragment>
      {/* Main record row */}
      <TableRow hover onClick={handleToggleExpand} sx={{ cursor: "pointer" }}>
        <TableCell width="50px">
          <IconButton size="small">
            {isExpanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell>{record.name}</TableCell>
        <TableCell>
          <RecordTypeChip type={record.record_type} />
        </TableCell>
        <TableCell>
          <StatusChip status={record.status} />
        </TableCell>
        <TableCell>
          <Button
            variant="contained"
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              handleViewClick(record.record_id);
            }}
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
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={4}>
          <Collapse in={isExpanded} timeout="auto" unmountOnExit>
            <Box m={3} display="flex" flexDirection="column" gap={2}>
              {record.description && (
                <Box>
                  <Typography variant="body2" fontWeight="bold">
                    Description
                  </Typography>
                  <Typography variant="body2">
                    {record.description}
                  </Typography>
                </Box>
              )}
              {record.tags && record.tags.length > 0 && (
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
                    {record.tags.map((tag) => (
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

export default function ProjectRecordTable({
  recordMetadata,
  onDelete,
}: ProjectRecordTableProps) {
  const { projectId } = useParams();
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [filter, setFilter] = useState("");

  const filteredRecords = React.useMemo(() => {
    return recordMetadata.filter((record) =>
      record.name.toLowerCase().includes(filter.toLowerCase()),
    );
  }, [recordMetadata, filter]);

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
      <Box sx={{ mb: 2 }} width={"30%"}>
        <TextField
          fullWidth
          variant="outlined"
          size="small"
          label="Filter records"
          value={filter}
          onChange={handleFilterChange}
        />
      </Box>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small" aria-label="record table">
          <TableHead>
            <TableRow>
              <TableCell width="50px" />
              <TableCell>
                Name
              </TableCell>
              <TableCell>
                Type
              </TableCell>
              <TableCell>
                Status
              </TableCell>
              <TableCell>
                Actions
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filteredRecords
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((record) => (
                <RecordRow
                  key={record.record_id}
                  record={record}
                  onDelete={onDelete}
                  projectId={projectId}
                />
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 25, 50]}
        component="div"
        count={filteredRecords.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </Box>
  );
}
