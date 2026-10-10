import React from "react";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { matchesTokens } from "../Utils.ts";
import { usePageTitle } from "../UsePageTitle.ts";
import {
  Box,
  Chip,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
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
  Button,
  IconButton,
  Collapse,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { FavoriteButton } from "../components/FavoriteButton.tsx";
import MarkdownContent from "../components/MarkdownContent";

const DatasetRow: React.FC<{ dataset: qcpTypes.DatasetListEntry }> = ({
  dataset,
}) => {
  const { makeRequest } = usePortalClient();
  const [isExpanded, setIsExpanded] = React.useState(false);

  const {
    status,
    data: datasetDetails,
    error,
  } = useQuery({
    queryKey: ["dataset", dataset.id],
    queryFn: () =>
      makeRequest<qcpTypes.Dataset>("GET", `/api/v1/datasets/${dataset.id}`),
    enabled: isExpanded,
  });

  const handleToggleExpand = () => {
    setIsExpanded((prev) => !prev);
  };

  return (
    <React.Fragment>
      <TableRow
        key={dataset.id}
        hover
        onClick={handleToggleExpand}
        sx={{ cursor: "pointer" }}
      >
        <TableCell width="50px">
          <IconButton size="small">
            {isExpanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell>
          <Stack direction="row" spacing={1} alignItems="center">
            <FavoriteButton
              preferencesKey="favorite_datasets"
              objectId={dataset.id}
            />
            <Typography fontWeight={"bold"}>{dataset.id}</Typography>
          </Stack>
        </TableCell>
        <TableCell>
          <Box sx={{ display: "flex", alignItems: "center" }}>
            <Box>
              <Typography variant="body2" fontWeight="bold">
                {dataset.dataset_name}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {dataset.tagline}
              </Typography>
            </Box>
          </Box>
        </TableCell>
        <TableCell>
          <Chip label={dataset.dataset_type} size="small" />
        </TableCell>
        <TableCell>{dataset.record_count.toLocaleString()}</TableCell>
        <TableCell>
          <Button
            variant="contained"
            size="small"
            component={Link}
            to={`/datasets/${dataset.id}`}
            onClick={(e) => {
              e.stopPropagation();
            }}
          >
            View
          </Button>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={7}>
          <Collapse in={isExpanded} timeout="auto" unmountOnExit>
            <Box m={3} display="flex" flexDirection="column" gap={2}>
              {status === "pending" && <LoadingIndicator />}
              {status === "error" && (
                <ErrorIndicator message={error.message} />
              )}
              {status === "success" && datasetDetails && (
                <>
                  {datasetDetails.description && (
                    <Box>
                      <Typography variant="body2" fontWeight="bold">
                        Description
                      </Typography>
                      <Typography variant="body2" component="div">
                        <MarkdownContent>
                          {datasetDetails.description.trim()}
                        </MarkdownContent>
                      </Typography>
                    </Box>
                  )}
                  {!datasetDetails.description && (
                    <Box>
                      <Typography variant="body2" fontWeight="bold">
                        Description
                      </Typography>
                      <Typography variant="body2" component="div">
                        No description available for this dataset.
                      </Typography>
                    </Box>
                  )}
                </>
              )}
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

const DatasetList: React.FC = () => {
  usePageTitle("Datasets");
  const { makeRequest } = usePortalClient();

  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(20);
  const [filter, setFilter] = React.useState("");
  const [typeFilter, setTypeFilter] = React.useState<string>("all");

  const {
    status,
    data: datasets,
    error,
  } = useQuery({
    queryKey: ["listDatasets"],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetListEntry[]>("GET", "/api/v1/datasets"),
  });

  const filteredDatasets = React.useMemo(() => {
    if (!datasets) return [];
    const searchFilter = filter || "";
    return datasets.filter((dataset) => {
      const matchesSearch =
        matchesTokens(searchFilter, dataset.dataset_name) ||
        dataset.id.toString().includes(searchFilter);
      const matchesType =
        typeFilter === "all" || dataset.dataset_type === typeFilter;
      return matchesSearch && matchesType;
    });
  }, [datasets, filter, typeFilter]);

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

  const handleTypeFilterChange = (event: SelectChangeEvent) => {
    setTypeFilter(event.target.value as string);
    setPage(0);
  };

  if (status == "pending") {
    return <LoadingIndicator fullPage />;
  }

  if (status == "error") {
    return <ErrorIndicator fullPage message={error.message} />;
  }

  return (
    <>
      {/* Title / Heading */}
      <Box sx={{ mt: 4, mb: 2, width: "100%" }}>
        <Typography variant="h4" gutterBottom>
          Datasets
        </Typography>
      </Box>

      {/* Table wrapped in Paper for typical MUI look */}
      <Box sx={{ width: "100%" }}>
        <Box sx={{ mb: 2, display: "flex", gap: 2 }}>
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
          <Box width={"20%"}>
            <FormControl fullWidth size="small">
              <InputLabel id="type-filter-label">Dataset Type</InputLabel>
              <Select
                labelId="type-filter-label"
                id="type-filter"
                value={typeFilter}
                label="Dataset Type"
                onChange={handleTypeFilterChange}
              >
                <MenuItem value="all">All Types</MenuItem>
                <MenuItem value="singlepoint">Singlepoint</MenuItem>
                <MenuItem value="optimization">Optimization</MenuItem>
                <MenuItem value="torsiondrive">Torsiondrive</MenuItem>
                <MenuItem value="gridoptimization">Gridoptimization</MenuItem>
                <MenuItem value="reaction">Reaction</MenuItem>
                <MenuItem value="manybody">Manybody</MenuItem>
                <MenuItem value="neb">NEB</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </Box>
        <TableContainer component={Paper} variant="outlined">
          <Table size="medium">
            <TableHead>
              <TableRow>
                <TableCell width="50px" />
                <TableCell width="5%" align="center">
                  ID
                </TableCell>
                <TableCell width="65%">Dataset Name</TableCell>
                <TableCell width="10%">Type</TableCell>
                <TableCell width="10%">Records</TableCell>
                <TableCell width="10%">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredDatasets
                .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                .map((dataset) => (
                  <DatasetRow key={dataset.id} dataset={dataset} />
                ))}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          rowsPerPageOptions={[10, 20, 50]}
          component="div"
          count={filteredDatasets.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
          sx={{ width: "100%" }}
        />
      </Box>
    </>
  );
};

export default DatasetList;
