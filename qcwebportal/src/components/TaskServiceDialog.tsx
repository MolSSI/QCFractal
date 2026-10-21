import React, { useState } from "react";
import * as qcpTypes from "../PortalTypes";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Button,
  Dialog,
  DialogContent,
  IconButton,
  Link as MuiLink,
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
import CloseIcon from "@mui/icons-material/Close";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import { GenericDataList } from "./GenericDataList.tsx";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import RecordTypeChip from "./RecordTypeChip.tsx";
import StatusChip from "./StatusChip.tsx";
import ManagerLink from "./ManagerLink.tsx";
import { dateStringToLocalTime } from "../Utils.ts";

interface TaskServiceDetailsDialogProps {
  recordId: number;
  recordType: string;
  isService: boolean;
  open: boolean;
  onClose: () => void;
}

type TaskServiceData = qcpTypes.RecordTask | qcpTypes.RecordService;

type DependencyRecordMetadata = Pick<
  qcpTypes.BaseRecord,
  | "id"
  | "record_type"
  | "status"
  | "manager_name"
  | "created_on"
  | "modified_on"
>;

type DependencyRow = DependencyRecordMetadata & {
  extras: Record<string, unknown>;
};

function renderExtraValue(value: unknown): React.ReactNode {
  if (Array.isArray(value)) {
    return value.length > 0 ? value.join(", ") : "[]";
  }

  if (value === null || value === undefined || value === "") {
    return "None";
  }

  if (typeof value === "object") {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      return "{}";
    }

    return entries
      .map(([key, entryValue]) => `${key}: ${String(entryValue)}`)
      .join(", ");
  }

  return String(value);
}

function ExtrasCell({
  extras,
  rowId,
}: {
  extras: Record<string, unknown>;
  rowId: number;
}) {
  const [expanded, setExpanded] = useState(false);
  const entries = Object.entries(extras);
  const visibleEntries = expanded ? entries : entries.slice(0, 3);
  const hasHiddenEntries = entries.length > 3;

  if (entries.length === 0) {
    return <Typography variant="body2">None</Typography>;
  }

  return (
    <Box>
      {visibleEntries.map(([key, value]) => (
        <Typography
          key={`${rowId}-${key}`}
          variant="body2"
          sx={{ overflowWrap: "anywhere" }}
        >
          <strong>{key}:</strong> {renderExtraValue(value)}
        </Typography>
      ))}
      {hasHiddenEntries && (
        <MuiLink
          component="button"
          type="button"
          variant="body2"
          onClick={() => setExpanded((prev) => !prev)}
          sx={{ mt: 0.5 }}
        >
          {expanded ? "Show less" : `Show ${entries.length - 3} more`}
        </MuiLink>
      )}
    </Box>
  );
}

function DependenciesTable({ rows }: { rows: DependencyRow[] }) {
  if (rows.length === 0) {
    return (
      <Typography variant="body2" color="text.secondary">
        No dependency records available.
      </Typography>
    );
  }

  return (
    <TableContainer component={Paper} variant="outlined" sx={{ mt: 1 }}>
      <Table size="small" aria-label="service dependency table">
        <TableHead>
          <TableRow>
            <TableCell>Record ID</TableCell>
            <TableCell>Record Type</TableCell>
            <TableCell>Status</TableCell>
            <TableCell width={"200"}>Manager Name</TableCell>
            <TableCell>Created/Modified On</TableCell>
            <TableCell>Extras</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.id}>
              <TableCell>
                <MuiLink
                  to={`/records/${row.id}`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  {row.id}
                </MuiLink>
              </TableCell>
              <TableCell>
                <RecordTypeChip type={row.record_type} />
              </TableCell>
              <TableCell>
                <StatusChip
                  status={row.status}
                  recordType={row.record_type}
                  recordId={row.id}
                />
              </TableCell>
              <TableCell>
                {row.manager_name ? (
                  <ManagerLink managerName={row.manager_name} />
                ) : (
                  <Typography>(none)</Typography>
                )}
              </TableCell>
              <TableCell>
                <Stack direction={"column"} alignItems={"flex-start"}>
                  <Typography>
                    {dateStringToLocalTime(row.created_on)}
                  </Typography>
                  <Typography>
                    {dateStringToLocalTime(row.modified_on)}
                  </Typography>
                </Stack>
              </TableCell>
              <TableCell>
                <ExtrasCell extras={row.extras} rowId={row.id} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}

function ServiceStateDetails({
  serviceState,
}: {
  serviceState: Record<string, unknown> | null | undefined;
}) {
  if (!serviceState || Object.keys(serviceState).length === 0) {
    return (
      <Typography variant="body2" color="text.secondary">
        No service state available.
      </Typography>
    );
  }

  return (
    <GenericDataList
      data={serviceState}
      keys={Object.keys(serviceState).filter(
        (key) => key !== "function_kwargs_compressed",
      )}
    />
  );
}

export const TaskServiceDialog: React.FC<TaskServiceDetailsDialogProps> = ({
  recordId,
  recordType,
  isService,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();
  const [serviceStateExpanded, setServiceStateExpanded] = useState(false);
  const [dependenciesExpanded, setDependenciesExpanded] = useState(false);

  const type = isService ? "service" : "task";

  const {
    status: taskStatus,
    data: taskData,
    error: taskError,
  } = useQuery({
    queryKey: ["recordTask", recordType, recordId],
    queryFn: () =>
      makeRequest<TaskServiceData>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/${type}`,
      ),
    enabled: open,
  });

  const handleClose = () => {
    setServiceStateExpanded(false);
    setDependenciesExpanded(false);
    onClose();
  };

  const serviceDependencies =
    isService && taskData && "dependencies" in taskData
      ? taskData.dependencies
      : [];
  const dependencyIds = serviceDependencies.map(
    (dependency) => dependency.record_id,
  );

  const {
    status: dependencyStatus,
    data: dependencyRows,
    error: dependencyError,
  } = useQuery({
    queryKey: ["recordServiceDependencies", recordId, dependencyIds],
    queryFn: async () => {
      const records = await makeRequest<DependencyRecordMetadata[]>(
        "POST",
        "api/v1/records/bulkGet",
        {
          ids: dependencyIds,
          include: [
            "record_type",
            "status",
            "manager_name",
            "created_on",
            "modified_on",
          ],
        },
      );

      const recordById = new Map(records.map((record) => [record.id, record]));

      return serviceDependencies.reduce<DependencyRow[]>((rows, dependency) => {
        const record = recordById.get(dependency.record_id);
        if (!record) {
          return rows;
        }

        rows.push({
          ...record,
          extras: (dependency.extras as Record<string, unknown>) ?? {},
        });

        return rows;
      }, []);
    },
    enabled:
      open &&
      isService &&
      dependenciesExpanded &&
      dependencyIds.length > 0 &&
      taskStatus === "success",
  });

  const detailKeys = taskData
    ? Object.keys(taskData).filter(
        (key) =>
          key !== "function_kwargs_compressed" &&
          key !== "dependencies" &&
          key !== "service_state",
      )
    : [];

  const serviceState =
    isService && taskData && "service_state" in taskData
      ? (taskData.service_state as Record<string, unknown> | null | undefined)
      : undefined;

  return (
    <Dialog
      fullWidth
      maxWidth="xl"
      open={open}
      onClose={handleClose}
      onClick={(e) => {
        e.stopPropagation();
      }}
    >
      <DialogContent sx={{ position: "relative", pt: 5 }}>
        <IconButton
          size="small"
          onClick={handleClose}
          sx={{
            position: "absolute",
            right: 8,
            top: 8,
            "&&": { bgcolor: "transparent", border: "none" },
            "&&:hover": { bgcolor: "transparent", border: "none" },
            "&&:active": { bgcolor: "transparent" },
          }}
        >
          <CloseIcon fontSize="small" />
        </IconButton>
        <Box>
          <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
            {type.charAt(0).toUpperCase() + type.slice(1)} Details
          </Typography>

          {taskStatus === "pending" && <LoadingIndicator />}

          {taskStatus === "error" && (
            <ErrorIndicator message={(taskError as any).message} />
          )}

          {taskStatus === "success" && taskData && (
            <>
              <GenericDataList
                data={taskData as Record<string, any>}
                keys={detailKeys}
              />

              {isService && (
                <>
                  <Accordion
                    expanded={serviceStateExpanded}
                    onChange={(_event, expanded) => {
                      setServiceStateExpanded(expanded);
                    }}
                    disableGutters
                    sx={{ mt: 2 }}
                  >
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Typography variant="subtitle1" fontWeight="bold">
                        Service State
                      </Typography>
                    </AccordionSummary>
                    <AccordionDetails>
                      <ServiceStateDetails serviceState={serviceState} />
                    </AccordionDetails>
                  </Accordion>

                  <Accordion
                    expanded={dependenciesExpanded}
                    onChange={(_event, expanded) => {
                      setDependenciesExpanded(expanded);
                    }}
                    disableGutters
                    sx={{ mt: 2 }}
                  >
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Typography variant="subtitle1" fontWeight="bold">
                        Dependencies ({serviceDependencies.length})
                      </Typography>
                    </AccordionSummary>
                    <AccordionDetails>
                      {dependencyStatus === "pending" &&
                        serviceDependencies.length > 0 && (
                          <LoadingIndicator message="Loading dependency records..." />
                        )}

                      {dependencyStatus === "error" && (
                        <ErrorIndicator
                          message={(dependencyError as Error).message}
                        />
                      )}

                      {(dependencyStatus === "success" ||
                        serviceDependencies.length === 0) && (
                        <DependenciesTable rows={dependencyRows ?? []} />
                      )}
                    </AccordionDetails>
                  </Accordion>
                </>
              )}
            </>
          )}
          {taskStatus === "success" && !taskData && (
            <Typography>No {type} data available for this record.</Typography>
          )}
        </Box>
      </DialogContent>
    </Dialog>
  );
};

interface TaskServiceButtonProps {
  recordId: number;
  recordType: string;
  isService: boolean;
  disabled: boolean;
}

export const TaskServiceButton: React.FC<TaskServiceButtonProps> = ({
  recordId,
  recordType,
  isService,
  disabled,
}) => {
  const [taskServiceDialogOpen, setTaskServiceDialogOpen] = useState(false);

  const buttonText = isService ? "View Service" : "View Task";

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        disabled={disabled}
        onClick={(e) => {
          e.stopPropagation();
          setTaskServiceDialogOpen(true);
        }}
      >
        {buttonText}
      </Button>

      <TaskServiceDialog
        recordId={recordId}
        recordType={recordType}
        isService={isService}
        open={taskServiceDialogOpen}
        onClose={() => setTaskServiceDialogOpen(false)}
      />
    </>
  );
};
