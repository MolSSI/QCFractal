import React from "react";
import {
  Box,
  Button,
  Chip,
  FormControl,
  IconButton,
  InputLabel,
  LinearProgress,
  Menu,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import DownloadIcon from "@mui/icons-material/Download";
import StopCircleIcon from "@mui/icons-material/StopCircle";
import {
  useInfiniteQuery,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { usePortalClient } from "../../PortalClient.tsx";
import * as qcpTypes from "../../PortalTypes";
import { downloadFile } from "../../Utils";
import ErrorIndicator from "../ErrorIndicator";
import JsonViewer, { JSON_VIEWER_COPY_BUTTON_CLASS } from "../JsonViewer";
import LoadingIndicator from "../LoadingIndicator";
import { PROPERTIES_ROWS_PER_PAGE_OPTIONS } from "./DatasetViewState";

interface DatasetPropertiesProps {
  datasetId: number;
  datasetType: qcpTypes.RecordType;
  specifications: string[];
  selectedProperties: string[];
  selectedSpecifications: string[];
  page: number;
  rowsPerPage: number;
  onSelectedPropertiesChange: (selectedProperties: string[]) => void;
  onSelectedSpecificationsChange: (selectedSpecifications: string[]) => void;
  onPageChange: (page: number) => void;
  onRowsPerPageChange: (rowsPerPage: number) => void;
  loadControl: PropertiesLoadControl;
  onLoadControlChange: (loadControl: PropertiesLoadControl) => void;
}

// Which selection (by query key) the user stopped loading, or let load past
// the automatic pause. Kept by the dataset page, so it survives switching tabs.
export type PropertiesLoadControl = {
  stoppedQueryKey: string | null;
  uncappedQueryKey: string | null;
};

// Loading pauses automatically after this many records. The browser can't
// comfortably hold millions of values; qcportal is better for those.
const AUTO_PAUSE_RECORDS = 200_000;

// While loading, the table is rebuilt at most this often
const TABLE_REFRESH_MS = 2000;

// Values by specification and entry name
type ValuesByEntrySpec = Map<string, unknown[]>;

const entrySpecKey = (specificationName: string, entryName: string) =>
  `${specificationName}\u0000${entryName}`;

// The values from the pages of one selection, added to as pages arrive
type AccumulatedValues = {
  queryKey: string;
  pages: PropertiesPage[];
  values: ValuesByEntrySpec;
  // Entries with values, in the order they arrived (sorted by name)
  entries: string[];
  entrySet: Set<string>;
};

// The values the table is currently built from
type ShownValues = {
  queryKey: string;
  values: ValuesByEntrySpec;
  entries: string[];
};

const NO_VALUES: ValuesByEntrySpec = new Map();
const NO_ENTRIES: string[] = [];

const sameItems = (a: string[], b: string[]) =>
  a.length === b.length && a.every((item) => b.includes(item));

// A complete record's values for the selected properties, in the requested order
type PropertyRow = [
  entry: string,
  specification: string,
  recordId: number,
  values: unknown[],
];

// A table column: a property of one specification
type PropertyColumn = { propertyIndex: number; numeric: boolean };

// (entry name, specification name) of the last row of the previous page
type PropertiesCursor = [string, string];

type PropertiesPage = {
  rows: PropertyRow[];
  next_cursor: PropertiesCursor | null;
  // Only requested (and so only present) on the first page
  total: number | null;
};

// Render a property value for a table cell, mimicking the precision of a
// pandas-rendered DataFrame. Missing values show as an em dash.
const formatPropertyValue = (value: unknown): string => {
  if (value === null || value === undefined || value === "") {
    return "—";
  }

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return String(value);
    }

    if (value === 0) {
      return "0";
    }

    const abs = Math.abs(value);
    if (abs >= 1e6 || abs < 1e-4) {
      return value.toExponential(4);
    }

    return String(Number(value.toPrecision(8)));
  }

  if (Array.isArray(value)) {
    return value.length > 0 ? value.map(formatPropertyValue).join(", ") : "—";
  }

  return String(value);
};

// Short, flat arrays (such as a dipole) are shown inline as text. Longer or
// nested arrays and objects are shown in a collapsed JSON viewer.
const INLINE_ARRAY_MAX_LENGTH = 10;

const isInlinePropertyValue = (value: unknown): boolean =>
  typeof value !== "object" ||
  value === null ||
  (Array.isArray(value) &&
    value.length <= INLINE_ARRAY_MAX_LENGTH &&
    value.every((item) => typeof item !== "object" || item === null));

// A value as a CSV field: full precision, arrays and objects as JSON, and
// missing values empty. Quoted when needed. Text that a spreadsheet would
// run as a formula (starting with =, +, -, @) is prefixed with ', except
// for numbers stored as text.
const toCsvField = (value: unknown): string => {
  let text =
    value === null || value === undefined
      ? ""
      : typeof value === "object"
        ? JSON.stringify(value)
        : String(value);
  if (
    typeof value === "string" &&
    /^[=+\-@\t\r]/.test(text) &&
    !isPropertyValueNumeric(value)
  ) {
    text = `'${text}`;
  }
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
};

const renderPropertyValue = (value: unknown): React.ReactNode =>
  isInlinePropertyValue(value) ? (
    formatPropertyValue(value)
  ) : (
    // The copy button shown on hover goes in the cell's padding, just after
    // the line, rather than widening the column (which would make the table
    // jump as the pointer moves down the column). The viewer's own scrolling
    // is turned off so it doesn't clip the button; the column grows instead.
    <Box
      sx={{
        "& > .MuiBox-root": { overflowX: "visible" },
        [`& :has(> .${JSON_VIEWER_COPY_BUTTON_CLASS})`]: {
          position: "relative",
        },
        [`& .${JSON_VIEWER_COPY_BUTTON_CLASS}`]: {
          position: "absolute",
          left: "100%",
          top: "50%",
          transform: "translateY(-50%)",
          // Padding rather than a margin, so moving the pointer from the line
          // onto the button never leaves the line (which hides the button)
          ml: 0,
          pl: 0.25,
        },
      }}
    >
      <JsonViewer value={value as object} expandDepth={0} />
    </Box>
  );

// Used for column alignment. Some programs store numbers as strings (such as
// "-0.06888645"), so those count as numbers too.
const isPropertyValueNumeric = (value: unknown): boolean =>
  (typeof value === "number" && Number.isFinite(value)) ||
  (typeof value === "string" &&
    value.trim() !== "" &&
    Number.isFinite(Number(value)));

// Long property/specification names would stretch the select boxes and table,
// so chips truncate with an ellipsis and show the full name on hover.
const CHIP_LABEL_MAX_WIDTH = 200;

// The chip area inside the multi-selects. No max height: the select grows
// with its content (see growWithChips in shared-theme/customizations/
// inputs.tsx), so no chip row is ever clipped or hidden behind a scrollbar.
const CHIP_AREA_SX = {
  display: "flex",
  flexWrap: "wrap",
  gap: 0.5,
  py: 0.25,
};

const TRUNCATED_CHIP_SX = {
  maxWidth: CHIP_LABEL_MAX_WIDTH,
  "& .MuiChip-label": {
    display: "block",
    overflow: "hidden",
    textOverflow: "ellipsis",
  },
};

function SelectChip({
  label,
  onDelete,
}: {
  label: string;
  onDelete: () => void;
}) {
  return (
    <Chip
      label={label}
      size="small"
      title={label}
      onDelete={onDelete}
      onMouseDown={(event) => {
        // Let the delete icon work without opening the dropdown
        event.stopPropagation();
      }}
      sx={TRUNCATED_CHIP_SX}
    />
  );
}

export default function DatasetProperties({
  datasetId,
  datasetType,
  specifications,
  selectedProperties,
  selectedSpecifications,
  page,
  rowsPerPage,
  onSelectedPropertiesChange,
  onSelectedSpecificationsChange,
  onPageChange,
  onRowsPerPageChange,
  loadControl,
  onLoadControlChange,
}: DatasetPropertiesProps) {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  // Which properties each specification typically computes (from complete records)
  const computedPropertiesQuery = useQuery({
    queryKey: ["datasetComputedProperties", datasetType, datasetId],
    queryFn: () =>
      makeRequest<Record<string, string[]>>(
        "GET",
        `api/v1/datasets/${datasetType}/${datasetId}/computed_properties`,
      ),
    enabled: !!datasetId && !!datasetType,
    // Slow for large datasets, so only refetched on an explicit refresh of
    // the dataset (which invalidates this query)
    staleTime: Infinity,
  });

  const propertyOptions = React.useMemo(() => {
    const options = new Set<string>();
    for (const properties of Object.values(
      computedPropertiesQuery.data ?? {},
    )) {
      properties.forEach((propertyName) => options.add(propertyName));
    }
    return Array.from(options).sort();
  }, [computedPropertiesQuery.data]);

  // Saved selections can name properties or specifications that are no
  // longer available, so drop those once the options are known
  React.useEffect(() => {
    if (!computedPropertiesQuery.data) {
      return;
    }
    const available = selectedProperties.filter((propertyName) =>
      propertyOptions.includes(propertyName),
    );
    if (available.length !== selectedProperties.length) {
      onSelectedPropertiesChange(available);
    }
  }, [
    computedPropertiesQuery.data,
    propertyOptions,
    selectedProperties,
    onSelectedPropertiesChange,
  ]);

  React.useEffect(() => {
    const available = selectedSpecifications.filter((specification) =>
      specifications.includes(specification),
    );
    if (available.length !== selectedSpecifications.length) {
      onSelectedSpecificationsChange(available);
    }
  }, [specifications, selectedSpecifications, onSelectedSpecificationsChange]);

  // While a dropdown is open, its selection is a draft, applied when the
  // dropdown closes. Choosing several items then starts one fetch, rather
  // than one per item.
  const [draftProperties, setDraftProperties] = React.useState<string[] | null>(
    null,
  );
  const [draftSpecifications, setDraftSpecifications] = React.useState<
    string[] | null
  >(null);

  // Column groups, in dataset order. An empty selection means every specification
  const selectedSpecs = React.useMemo(
    () =>
      selectedSpecifications.length === 0
        ? specifications
        : specifications.filter((specification) =>
            selectedSpecifications.includes(specification),
          ),
    [specifications, selectedSpecifications],
  );

  // Sorted for a stable query key regardless of selection order
  const sortedSelectedProperties = React.useMemo(
    () => [...selectedProperties].sort(),
    [selectedProperties],
  );

  const sortedSelectedSpecifications = React.useMemo(
    () => [...selectedSpecifications].sort(),
    [selectedSpecifications],
  );

  const valuesQueryKey = [
    "datasetPropertiesValues",
    datasetType,
    datasetId,
    sortedSelectedProperties,
    sortedSelectedSpecifications,
  ];

  // The server extracts the selected properties from complete records, a page
  // (of up to its get_records limit) at a time
  const valuesQuery = useInfiniteQuery({
    queryKey: valuesQueryKey,
    initialPageParam: null as PropertiesCursor | null,
    enabled: sortedSelectedProperties.length > 0,
    queryFn: ({ pageParam }) =>
      makeRequest<PropertiesPage>(
        "POST",
        `api/v1/datasets/${datasetType}/${datasetId}/properties/bulkFetch`,
        {
          properties: sortedSelectedProperties,
          specification_names:
            sortedSelectedSpecifications.length > 0
              ? sortedSelectedSpecifications
              : null,
          cursor: pageParam,
          include_total: pageParam === null,
        },
      ),
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    // Every refetch walks all pages again, so only refetch on an explicit
    // refresh of the dataset (which invalidates this query)
    staleTime: Infinity,
  });

  const valuesQueryKeyString = JSON.stringify(valuesQueryKey);

  // Only the current selection's values are kept, since they can be large
  React.useEffect(() => {
    queryClient.removeQueries({
      queryKey: ["datasetPropertiesValues", datasetType, datasetId],
      predicate: (query) =>
        JSON.stringify(query.queryKey) !== valuesQueryKeyString,
    });
  }, [queryClient, datasetType, datasetId, valuesQueryKeyString]);

  const pages = valuesQuery.data?.pages;
  const totalRowCount = pages?.[0]?.total ?? 0;
  const loadedRowCount = React.useMemo(
    () => (pages ?? []).reduce((count, page) => count + page.rows.length, 0),
    [pages],
  );

  // Loading is paused when the user stops it, or automatically after
  // AUTO_PAUSE_RECORDS (unless the user chose to continue)
  const isStopped = loadControl.stoppedQueryKey === valuesQueryKeyString;
  const isAutoPaused =
    loadControl.uncappedQueryKey !== valuesQueryKeyString &&
    loadedRowCount >= AUTO_PAUSE_RECORDS;
  const isPaused = isStopped || isAutoPaused;

  // Fetch the following pages until done or paused. Waits for any fetch (not
  // just next-page fetches): fetchNextPage would otherwise cancel a refetch.
  const { isSuccess, isFetching, hasNextPage, fetchNextPage } = valuesQuery;
  React.useEffect(() => {
    if (!isSuccess || isFetching || !hasNextPage || isPaused) {
      return;
    }

    void fetchNextPage();
  }, [isSuccess, isFetching, hasNextPage, isPaused, fetchNextPage]);

  const isLoadingMore = isFetching || (hasNextPage && !isPaused);

  // The values the table is built from. New pages are added to the values
  // already loaded, rather than starting over for every page (it starts over
  // for a new selection, or when the pages are refetched). Since rebuilding
  // the table touches every value, it's updated at most every
  // TABLE_REFRESH_MS while loading, and right away otherwise.
  const accumulatedRef = React.useRef<AccumulatedValues | null>(null);
  const lastShownAtRef = React.useRef(0);
  const [shownValues, setShownValues] = React.useState<ShownValues>({
    queryKey: valuesQueryKeyString,
    values: new Map(),
    entries: [],
  });
  React.useEffect(() => {
    const previous = accumulatedRef.current;
    const currentPages = pages ?? [];
    const continues =
      previous !== null &&
      previous.queryKey === valuesQueryKeyString &&
      previous.pages.length <= currentPages.length &&
      previous.pages.every((page, index) => page === currentPages[index]);

    const values: ValuesByEntrySpec = continues
      ? previous.values
      : new Map<string, unknown[]>();
    const entries = continues ? previous.entries : [];
    const entrySet = continues ? previous.entrySet : new Set<string>();
    for (const page of currentPages.slice(
      continues ? previous.pages.length : 0,
    )) {
      for (const [entryName, specificationName, , rowValues] of page.rows) {
        values.set(entrySpecKey(specificationName, entryName), rowValues);
        if (!entrySet.has(entryName)) {
          entrySet.add(entryName);
          entries.push(entryName);
        }
      }
    }
    accumulatedRef.current = {
      queryKey: valuesQueryKeyString,
      pages: currentPages,
      values,
      entries,
      entrySet,
    };

    const show = () => {
      lastShownAtRef.current = Date.now();
      // Copies, since the accumulated values keep changing as pages arrive
      setShownValues({
        queryKey: valuesQueryKeyString,
        values: new Map(values),
        entries: [...entries],
      });
    };
    const wait =
      isLoadingMore && continues
        ? lastShownAtRef.current + TABLE_REFRESH_MS - Date.now()
        : 0;
    if (wait <= 0) {
      show();
      return;
    }

    const timer = setTimeout(show, wait);
    return () => clearTimeout(timer);
  }, [pages, valuesQueryKeyString, isLoadingMore]);

  // Never show values from a previous selection, even for a moment
  const isCurrentSelection = shownValues.queryKey === valuesQueryKeyString;
  const tableValues = isCurrentSelection ? shownValues.values : NO_VALUES;
  const tableEntries = isCurrentSelection ? shownValues.entries : NO_ENTRIES;

  // Pivot to entry rows with (specification, property) columns, dropping
  // columns where every value is missing — like get_properties_df's dropna.
  // Rows are the loaded entries, in the order they arrived (sorted by name).
  const pivot = React.useMemo(() => {
    const entries = tableEntries;

    // Columns (property indices) with at least one value, by specification.
    // A column is right-aligned (header included) if all its values are numbers
    const columnsBySpec = new Map<string, PropertyColumn[]>();
    for (const specificationName of selectedSpecs) {
      const columns: PropertyColumn[] = [];
      sortedSelectedProperties.forEach((_, propertyIndex) => {
        const columnValues = entries
          .map(
            (entryName) =>
              tableValues.get(entrySpecKey(specificationName, entryName))?.[
                propertyIndex
              ],
          )
          .filter((value) => value !== null && value !== undefined);
        if (columnValues.length > 0) {
          columns.push({
            propertyIndex,
            numeric: columnValues.every(isPropertyValueNumeric),
          });
        }
      });
      if (columns.length > 0) {
        columnsBySpec.set(specificationName, columns);
      }
    }

    return { entries, columnsBySpec };
  }, [tableEntries, tableValues, selectedSpecs, sortedSelectedProperties]);

  const visibleColumnCount = Array.from(pivot.columnsBySpec.values()).reduce(
    (total, columns) => total + columns.length,
    0,
  );

  // Clamp rather than store: rows can drop away when the selection changes
  const pageCount = Math.max(1, Math.ceil(pivot.entries.length / rowsPerPage));
  const currentPage = Math.min(page, pageCount - 1);
  const visibleEntries = React.useMemo(
    () =>
      pivot.entries.slice(
        currentPage * rowsPerPage,
        currentPage * rowsPerPage + rowsPerPage,
      ),
    [pivot.entries, currentPage, rowsPerPage],
  );

  const progressValue =
    totalRowCount > 0
      ? Math.min(100, (loadedRowCount / totalRowCount) * 100)
      : 0;

  const handleSelectedPropertiesChange = (
    event: SelectChangeEvent<typeof selectedProperties>,
  ) => {
    const { value } = event.target;
    setDraftProperties(typeof value === "string" ? value.split(",") : value);
  };

  const handlePropertiesClose = () => {
    if (draftProperties && !sameItems(draftProperties, selectedProperties)) {
      onSelectedPropertiesChange(draftProperties);
    }
    setDraftProperties(null);
  };

  // Deleting a chip applies right away, even while the dropdown is open
  // (then also to the draft, so closing the dropdown doesn't restore it)
  const handleDeleteProperty = (propertyName: string) => {
    const remaining = (draftProperties ?? selectedProperties).filter(
      (property) => property !== propertyName,
    );
    if (draftProperties) {
      setDraftProperties(remaining);
    }
    onSelectedPropertiesChange(remaining);
  };

  const handleSelectedSpecificationsChange = (
    event: SelectChangeEvent<typeof selectedSpecifications>,
  ) => {
    const { value } = event.target;
    setDraftSpecifications(
      typeof value === "string" ? value.split(",") : value,
    );
  };

  const handleSpecificationsClose = () => {
    if (
      draftSpecifications &&
      !sameItems(draftSpecifications, selectedSpecifications)
    ) {
      onSelectedSpecificationsChange(draftSpecifications);
    }
    setDraftSpecifications(null);
  };

  const handleDeleteSpecification = (specificationName: string) => {
    const remaining = (draftSpecifications ?? selectedSpecifications).filter(
      (specification) => specification !== specificationName,
    );
    if (draftSpecifications) {
      setDraftSpecifications(remaining);
    }
    onSelectedSpecificationsChange(remaining);
  };

  const renderCell = (
    specificationName: string,
    entryName: string,
    propertyIndex: number,
  ): React.ReactNode =>
    renderPropertyValue(
      tableValues.get(entrySpecKey(specificationName, entryName))?.[
        propertyIndex
      ],
    );

  // Downloads are offered when nothing is loading: everything has loaded, or
  // loading was stopped (then the data and the file name say it's partial)
  const [downloadMenuAnchor, setDownloadMenuAnchor] =
    React.useState<HTMLElement | null>(null);
  const canDownload = !isLoadingMore && pivot.entries.length > 0;
  const isPartial = hasNextPage;

  const handleDownload = (format: "csv" | "json") => {
    setDownloadMenuAnchor(null);
    const filename = `dataset_${datasetId}_properties${isPartial ? "_partial" : ""}.${format}`;

    if (format === "csv") {
      // Like the table: one row per entry, one column per (specification, property)
      const columns = Array.from(pivot.columnsBySpec.entries()).flatMap(
        ([specificationName, specColumns]) =>
          specColumns.map(({ propertyIndex }) => ({
            specificationName,
            propertyIndex,
          })),
      );
      const header = [
        "entry",
        ...columns.map(
          ({ specificationName, propertyIndex }) =>
            `${specificationName} / ${sortedSelectedProperties[propertyIndex]}`,
        ),
      ];
      const lines = [
        header,
        ...pivot.entries.map((entryName) => [
          entryName,
          ...columns.map(
            ({ specificationName, propertyIndex }) =>
              tableValues.get(entrySpecKey(specificationName, entryName))?.[
                propertyIndex
              ],
          ),
        ]),
      ].map((fields) => fields.map(toCsvField).join(","));
      downloadFile(lines.join("\r\n") + "\r\n", filename, "text/csv");
    } else {
      // One object per record, with values as stored
      const records = (pages ?? [])
        .flatMap((page) => page.rows)
        .map(([entry, specification, recordId, values]) => ({
          entry,
          specification,
          record_id: recordId,
          properties: Object.fromEntries(
            sortedSelectedProperties.map((propertyName, propertyIndex) => [
              propertyName,
              values[propertyIndex] ?? null,
            ]),
          ),
        }));
      // Not indented, which would make large downloads much bigger
      downloadFile(JSON.stringify(records), filename, "application/json");
    }
  };

  if (computedPropertiesQuery.isPending) {
    return (
      <LoadingIndicator message="Finding the properties computed by each specification. This can take a while for large datasets..." />
    );
  }

  if (computedPropertiesQuery.isError) {
    return (
      <ErrorIndicator message="Failed to load computed properties for this dataset" />
    );
  }

  if (propertyOptions.length === 0) {
    return (
      <Typography>
        No computed properties available. Properties appear once records for
        this dataset complete.
      </Typography>
    );
  }

  return (
    <Box>
      <Box
        sx={{
          mb: 3,
          display: "flex",
          gap: 2,
          flexWrap: "wrap",
          alignItems: "center",
        }}
      >
        <FormControl size="small" sx={{ minWidth: 260, maxWidth: 480 }}>
          <InputLabel shrink>Properties</InputLabel>
          <Select
            multiple
            value={draftProperties ?? selectedProperties}
            label="Properties"
            onOpen={() => setDraftProperties(selectedProperties)}
            onChange={handleSelectedPropertiesChange}
            onClose={handlePropertiesClose}
            renderValue={(selected) => (
              <Box sx={CHIP_AREA_SX}>
                {(selected as string[]).map((value) => (
                  <SelectChip
                    key={value}
                    label={value}
                    onDelete={() => handleDeleteProperty(value)}
                  />
                ))}
              </Box>
            )}
          >
            {propertyOptions.map((propertyName) => (
              <MenuItem key={propertyName} value={propertyName}>
                {propertyName}
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 220, maxWidth: 480 }}>
          <InputLabel shrink>Specifications</InputLabel>
          <Select
            multiple
            value={draftSpecifications ?? selectedSpecifications}
            label="Specifications"
            onOpen={() => setDraftSpecifications(selectedSpecifications)}
            onChange={handleSelectedSpecificationsChange}
            onClose={handleSpecificationsClose}
            renderValue={(selected) =>
              (selected as string[]).length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  All Specifications
                </Typography>
              ) : (
                <Box sx={CHIP_AREA_SX}>
                  {(selected as string[]).map((value) => (
                    <SelectChip
                      key={value}
                      label={value}
                      onDelete={() => handleDeleteSpecification(value)}
                    />
                  ))}
                </Box>
              )
            }
          >
            {specifications.map((specification) => (
              <MenuItem key={specification} value={specification}>
                {specification}
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        {isLoadingMore && totalRowCount > 0 && (
          <Box sx={{ flexGrow: 1, minWidth: 260 }}>
            <Box
              sx={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                mb: 0.5,
                gap: 1,
              }}
            >
              <Typography variant="caption" color="text.secondary">
                Loaded {loadedRowCount.toLocaleString()} /{" "}
                {totalRowCount.toLocaleString()} records
              </Typography>
              <Tooltip title="Stop loading">
                <IconButton
                  size="small"
                  aria-label="Stop loading property values"
                  onClick={() =>
                    onLoadControlChange({
                      ...loadControl,
                      stoppedQueryKey: valuesQueryKeyString,
                    })
                  }
                >
                  <StopCircleIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
            <LinearProgress variant="determinate" value={progressValue} />
          </Box>
        )}

        {/* Takes the place of the progress bar, which only shows while loading */}
        {canDownload && (
          <Box sx={{ ml: "auto" }}>
            <Button
              size="small"
              startIcon={<DownloadIcon />}
              endIcon={<ArrowDropDownIcon />}
              onClick={(event) => setDownloadMenuAnchor(event.currentTarget)}
            >
              {isPartial ? "Download partial results" : "Download"}
            </Button>
            <Menu
              anchorEl={downloadMenuAnchor}
              open={downloadMenuAnchor !== null}
              onClose={() => setDownloadMenuAnchor(null)}
            >
              <MenuItem onClick={() => handleDownload("csv")}>
                CSV (one row per entry)
              </MenuItem>
              <MenuItem onClick={() => handleDownload("json")}>
                JSON (one object per record)
              </MenuItem>
            </Menu>
          </Box>
        )}
      </Box>

      {sortedSelectedProperties.length === 0 ? (
        <Typography>
          Select one or more properties to display their values for each entry.
        </Typography>
      ) : valuesQuery.isError ? (
        <ErrorIndicator message="Failed to load record properties" />
      ) : valuesQuery.isPending ? (
        <LoadingIndicator />
      ) : (
        <>
          {isPaused && hasNextPage && (
            <Box sx={{ mb: 1, display: "flex", alignItems: "center", gap: 1 }}>
              <Typography variant="body2" color="text.secondary">
                {isStopped
                  ? `Loading stopped early — showing partial results (${loadedRowCount.toLocaleString()} of ${totalRowCount.toLocaleString()} records).`
                  : `Loading paused after ${loadedRowCount.toLocaleString()} of ${totalRowCount.toLocaleString()} records. For this many, qcportal's get_properties_df (or a view of the dataset) is a better way to get all the values.`}
              </Typography>
              <Button
                size="small"
                sx={{ flexShrink: 0 }}
                onClick={() =>
                  onLoadControlChange(
                    isStopped
                      ? { ...loadControl, stoppedQueryKey: null }
                      : {
                          ...loadControl,
                          uncappedQueryKey: valuesQueryKeyString,
                        },
                  )
                }
              >
                {isStopped ? "Resume" : "Continue loading"}
              </Button>
            </Box>
          )}
          <TableContainer component={Paper} variant="outlined">
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell rowSpan={2} sx={{ whiteSpace: "nowrap" }}>
                    Entry
                  </TableCell>
                  {Array.from(pivot.columnsBySpec.entries()).map(
                    ([specificationName, columns]) => (
                      <TableCell
                        key={specificationName}
                        colSpan={columns.length}
                        align="center"
                        sx={{
                          borderLeft: 1,
                          borderColor: "divider",
                          whiteSpace: "nowrap",
                        }}
                        title={specificationName}
                      >
                        {specificationName}
                      </TableCell>
                    ),
                  )}
                </TableRow>
                <TableRow>
                  {Array.from(pivot.columnsBySpec.entries()).flatMap(
                    ([specificationName, columns]) =>
                      columns.map(({ propertyIndex, numeric }) => (
                        <TableCell
                          key={`${specificationName}-${propertyIndex}`}
                          align={numeric ? "right" : "left"}
                          sx={{
                            borderLeft: 1,
                            borderColor: "divider",
                            whiteSpace: "nowrap",
                          }}
                          title={sortedSelectedProperties[propertyIndex]}
                        >
                          {sortedSelectedProperties[propertyIndex]}
                        </TableCell>
                      )),
                  )}
                </TableRow>
              </TableHead>
              <TableBody>
                {visibleEntries.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={visibleColumnCount + 1} align="center">
                      {isLoadingMore
                        ? "Loading property values..."
                        : "No completed records found for the selected properties"}
                    </TableCell>
                  </TableRow>
                ) : (
                  visibleEntries.map((entryName) => (
                    <TableRow key={entryName}>
                      <TableCell
                        sx={{
                          maxWidth: 280,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                        title={entryName}
                      >
                        {entryName}
                      </TableCell>
                      {Array.from(pivot.columnsBySpec.entries()).flatMap(
                        ([specificationName, columns]) =>
                          columns.map(({ propertyIndex, numeric }) => (
                            <TableCell
                              key={`${specificationName}-${propertyIndex}`}
                              align={numeric ? "right" : "left"}
                              sx={{
                                borderLeft: 1,
                                borderColor: "divider",
                              }}
                            >
                              {renderCell(
                                specificationName,
                                entryName,
                                propertyIndex,
                              )}
                            </TableCell>
                          )),
                      )}
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>
          <TablePagination
            rowsPerPageOptions={PROPERTIES_ROWS_PER_PAGE_OPTIONS}
            component="div"
            count={pivot.entries.length}
            rowsPerPage={rowsPerPage}
            page={currentPage}
            onPageChange={(_event, newPage) => onPageChange(newPage)}
            onRowsPerPageChange={(event) => {
              onRowsPerPageChange(parseInt(event.target.value, 10));
            }}
          />
        </>
      )}
    </Box>
  );
}
