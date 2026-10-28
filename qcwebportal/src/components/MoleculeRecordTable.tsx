import React from "react";
import {
  FormControl,
  InputLabel,
  Link as MuiLink,
  MenuItem,
  Paper,
  Select,
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
import { keepPreviousData, useQueries, useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import { useAuth } from "../Auth.tsx";
import * as qcpTypes from "../PortalTypes.ts";
import LoadingIndicator from "./LoadingIndicator.tsx";
import ErrorIndicator from "./ErrorIndicator.tsx";
import { StatusChip } from "./StatusChip.tsx";
import { asRecord, dateStringToLocalTime } from "../Utils.ts";
import { getMoleculeRecordOptions } from "./record_components/lookup.tsx";

const moleculeRecordOptions = getMoleculeRecordOptions();
const MAX_COUNTED_RECORDS = 100;

type MoleculeRecordPage = {
  records: qcpTypes.RecordData[];
  hasMore: boolean;
};

// Each record type nests its QC specification differently
function findQCSpecification(
  spec: unknown,
): qcpTypes.QCSpecification | undefined {
  const specObj = asRecord(spec);
  if (!specObj) return undefined;
  if ("method" in specObj) return specObj as qcpTypes.QCSpecification;
  return (
    findQCSpecification(specObj.singlepoint_specification) ??
    findQCSpecification(specObj.qc_specification) ??
    findQCSpecification(specObj.optimization_specification)
  );
}

function summarizeSpecification(record: qcpTypes.RecordData): string {
  const qcSpec = findQCSpecification(record.specification);
  if (!qcSpec) return "";
  return [qcSpec.program, qcSpec.method, qcSpec.basis || "(none)"].join(" / ");
}

interface MoleculeRecordTableProps {
  moleculeId: number;
}

export default function MoleculeRecordTable({
  moleculeId,
}: MoleculeRecordTableProps) {
  const { makeRequest } = usePortalClient();
  const { serverInfo } = useAuth();

  const recordLimit = serverInfo.api_limits.get_records || 1000;
  const maxRecordsPerRequest = Math.max(1, Math.min(recordLimit, 200));
  // The server clamps query limits to get_records, so leave room for the
  // lookahead id that tells whether another page exists
  const maxPageSize = Math.max(1, recordLimit - 1);
  const allowedPageSizes = [10, 25, 50].filter((n) => n <= maxPageSize);
  const pageSizeOptions =
    allowedPageSizes.length > 0 ? allowedPageSizes : [maxPageSize];

  const [optionKey, setOptionKey] = React.useState(moleculeRecordOptions[0].key);
  const [requestedPageSize, setRequestedPageSize] = React.useState(10);
  const rowsPerPage = Math.min(requestedPageSize, maxPageSize);
  // pageCursors[i] is the query cursor for page i; records come newest first
  // and the cursor returns ids below it
  const [pageCursors, setPageCursors] = React.useState<(number | undefined)[]>(
    [undefined],
  );

  const page = pageCursors.length - 1;
  const cursor = pageCursors[page];
  const option =
    moleculeRecordOptions.find((o) => o.key === optionKey) ??
    moleculeRecordOptions[0];

  const resetPaging = () => setPageCursors([undefined]);

  // Newest-first ids per option, capped; "N+" when the cap is hit. They also
  // serve as the first page of the table, which is always smaller than the cap
  const countLimit = Math.min(MAX_COUNTED_RECORDS, recordLimit);
  const countQueries = useQueries({
    queries: moleculeRecordOptions.map((o) => ({
      queryKey: ["moleculeRecordIds", moleculeId, o.key, countLimit],
      queryFn: () =>
        makeRequest<number[]>(
          "POST",
          `api/v1/records/${o.recordType}/query`,
          { ...o.extraFilters, [o.field]: [moleculeId], limit: countLimit },
        ),
      staleTime: 5 * 60 * 1000,
    })),
  });

  const formatOptionCount = (ids: number[] | undefined) => {
    if (ids === undefined) return "…";
    return ids.length >= countLimit ? `${ids.length}+` : String(ids.length);
  };

  const countsLoading = countQueries.some((q) => q.isPending);
  const noRecords = countQueries.every((q) => q.data?.length === 0);

  const selectedCountQuery =
    countQueries[moleculeRecordOptions.indexOf(option)];
  const firstPageIds = cursor === undefined ? selectedCountQuery.data : undefined;

  const { status, data, error, isFetching } = useQuery({
    queryKey: [
      "moleculeRecords",
      moleculeId,
      option.key,
      cursor,
      rowsPerPage,
      firstPageIds,
    ],
    queryFn: async (): Promise<MoleculeRecordPage> => {
      let ids = firstPageIds;
      if (ids === undefined) {
        const queryBody: Record<string, unknown> = {
          ...option.extraFilters,
          [option.field]: [moleculeId],
          limit: rowsPerPage + 1,
        };
        if (cursor !== undefined) {
          queryBody.cursor = cursor;
        }
        ids = await makeRequest<number[]>(
          "POST",
          `api/v1/records/${option.recordType}/query`,
          queryBody,
        );
      }

      const pageIds = ids.slice(0, rowsPerPage);
      const records: qcpTypes.RecordData[] = [];
      for (let i = 0; i < pageIds.length; i += maxRecordsPerRequest) {
        const batch = await makeRequest<qcpTypes.RecordData[]>(
          "POST",
          `api/v1/records/${option.recordType}/bulkGet`,
          { ids: pageIds.slice(i, i + maxRecordsPerRequest), include: ["*"] },
        );
        records.push(...batch);
      }

      return { records, hasMore: ids.length > rowsPerPage };
    },
    placeholderData: keepPreviousData,
    enabled: cursor !== undefined || !selectedCountQuery.isPending,
  });

  const records = data?.records ?? [];
  const hasMore = data?.hasMore ?? false;
  const totalCount = hasMore ? -1 : page * rowsPerPage + records.length;

  const handlePageChange = (newPage: number) => {
    if (newPage > page) {
      const lastRecord = records[records.length - 1];
      if (!hasMore || !lastRecord) return;
      setPageCursors([...pageCursors, lastRecord.id]);
    } else {
      setPageCursors(pageCursors.slice(0, newPage + 1));
    }
  };

  if (countsLoading) {
    return (
      <>
        <Typography variant="h6" sx={{ mb: 2 }}>
          Records Using This Molecule
        </Typography>
        <LoadingIndicator message="Loading records..." />
      </>
    );
  }

  if (noRecords) {
    return (
      <>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Records Using This Molecule
        </Typography>
        <Typography color="text.secondary">
          There are currently no records that use this molecule.
        </Typography>
      </>
    );
  }

  return (
    <>
      <Stack
        direction="row"
        alignItems="center"
        flexWrap="wrap"
        gap={2}
        sx={{ mb: 2 }}
      >
        <Typography variant="h6">Records Using This Molecule</Typography>
        <FormControl size="small" sx={{ minWidth: 240 }}>
          <InputLabel id="molecule-record-type-label">Record type</InputLabel>
          <Select
            labelId="molecule-record-type-label"
            label="Record type"
            value={option.key}
            sx={{ textTransform: "capitalize" }}
            onChange={(e) => {
              setOptionKey(e.target.value);
              resetPaging();
            }}
          >
            {moleculeRecordOptions.map((o, i) => {
              const countQuery = countQueries[i];
              return (
                <MenuItem
                  key={o.key}
                  value={o.key}
                  disabled={countQuery.data?.length === 0}
                  sx={{ textTransform: "capitalize" }}
                >
                  {o.label}
                  {!countQuery.isError &&
                    ` (${formatOptionCount(countQuery.data)})`}
                </MenuItem>
              );
            })}
          </Select>
        </FormControl>
      </Stack>

      {status === "pending" && <LoadingIndicator message="Loading records..." />}

      {status === "error" && <ErrorIndicator message={error.message} />}

      {status === "success" && (
        <>
          <TableContainer
            component={Paper}
            variant="outlined"
            sx={{ opacity: isFetching ? 0.6 : 1 }}
          >
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>
                    <strong>Record ID</strong>
                  </TableCell>
                  <TableCell>
                    <strong>Name</strong>
                  </TableCell>
                  <TableCell>
                    <strong>Status</strong>
                  </TableCell>
                  <TableCell>
                    <strong>Specification</strong>
                  </TableCell>
                  <TableCell>
                    <strong>Created On</strong>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {records.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} align="center">
                      No {option.label} records use this molecule.
                    </TableCell>
                  </TableRow>
                ) : (
                  records.map((record) => (
                    <TableRow key={record.id}>
                      <TableCell>
                        <MuiLink to={`/records/${record.id}`}>
                          {record.id}
                        </MuiLink>
                      </TableCell>
                      <TableCell>{record.name}</TableCell>
                      <TableCell>
                        <StatusChip
                          status={record.status}
                          recordType={record.record_type}
                          recordId={record.id}
                        />
                      </TableCell>
                      <TableCell>{summarizeSpecification(record)}</TableCell>
                      <TableCell>
                        {dateStringToLocalTime(record.created_on)}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>
          {(page > 0 || records.length > 0) && (
            <TablePagination
              rowsPerPageOptions={pageSizeOptions}
              component="div"
              count={totalCount}
              rowsPerPage={rowsPerPage}
              page={page}
              onPageChange={(_e, newPage) => handlePageChange(newPage)}
              onRowsPerPageChange={(e) => {
                setRequestedPageSize(parseInt(e.target.value, 10));
                resetPaging();
              }}
              slotProps={{
                actions: {
                  nextButton: { disabled: !hasMore || isFetching },
                  previousButton: { disabled: page === 0 || isFetching },
                },
              }}
            />
          )}
        </>
      )}
    </>
  );
}
