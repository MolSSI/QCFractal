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
  Typography,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import * as qcpTypes from "../../PortalTypes";
import { getSpecificationComponent } from "../record_components/lookup.tsx";

interface DatasetSpecificationTableProps {
  specificationsData: Record<string, any>;
  datasetType: qcpTypes.RecordType;
}

export default function DatasetSpecificationTable({
  specificationsData,
  datasetType,
}: DatasetSpecificationTableProps) {
  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(10);
  const [expandedSpecName, setExpandedSpecName] = React.useState<string | null>(
    null,
  );

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const toggleExpandSpec = (name: string) => {
    setExpandedSpecName((prev) => (prev === name ? null : name));
  };

  const specNames = Object.keys(specificationsData);

  return (
    <>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell width="50px" />
              <TableCell>
                <strong>Name</strong>
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
