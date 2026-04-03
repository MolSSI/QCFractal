import React from "react";
import {
  Box,
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
import * as qcpTypes from "../../PortalTypes";
import { getSpecificationComponent } from "../record_components/lookup.tsx";

interface DatasetSpecificationTableProps {
  specificationsData: Record<string, { specification: qcpTypes.DatasetSpecificationData }>;
  datasetType: qcpTypes.RecordType;
}

export default function DatasetSpecificationTable({
  specificationsData,
  datasetType,
}: DatasetSpecificationTableProps) {
  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(10);
  const [filter, setFilter] = React.useState("");
  const [expandedSpecName, setExpandedSpecName] = React.useState<string | null>(
    null,
  );

  const specNames = React.useMemo(() => {
    return Object.keys(specificationsData).filter((name) =>
      name.toLowerCase().includes(filter.toLowerCase()),
    );
  }, [specificationsData, filter]);

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

  const toggleExpandSpec = (name: string) => {
    setExpandedSpecName((prev) => (prev === name ? null : name));
  };

  return (
    <>
      <Box sx={{ mb: 2 }} width={"30%"}>
        <TextField
          fullWidth
          variant="outlined"
          size="small"
          label="Filter specifications"
          value={filter}
          onChange={handleFilterChange}
        />
      </Box>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell width="50px" />
              <TableCell>
                Name
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {specNames
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((specName) => (
                <React.Fragment key={specName}>
                  <TableRow
                    hover
                    sx={{ cursor: "pointer" }}
                    onClick={() => toggleExpandSpec(specName)}
                  >
                    <TableCell>
                      <IconButton size="small">
                        {expandedSpecName === specName ? (
                          <KeyboardArrowUpIcon />
                        ) : (
                          <KeyboardArrowDownIcon />
                        )}
                      </IconButton>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" fontWeight="medium">
                        {specName}
                      </Typography>
                    </TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell
                      style={{ paddingBottom: 0, paddingTop: 0 }}
                      colSpan={2}
                    >
                      <Collapse
                        in={expandedSpecName === specName}
                        timeout="auto"
                        unmountOnExit
                      >
                        <Box>
                          {(() => {
                            const SpecificationComponent =
                              getSpecificationComponent(datasetType);
                            return (
                              <SpecificationComponent
                                specification={
                                  specificationsData[specName].specification
                                }
                              />
                            );
                          })()}
                        </Box>
                      </Collapse>
                    </TableCell>
                  </TableRow>
                </React.Fragment>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[10, 25, 50]}
        component="div"
        count={specNames.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </>
  );
}
