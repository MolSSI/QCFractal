import React, { useState, useCallback, useMemo, useRef, useEffect } from "react";
import {
  Box,
  Button,
  Dialog,
  DialogContent,
  IconButton,
  InputAdornment,
  Tab,
  Tabs,
  TextField,
  Tooltip,
  Typography,
  useMediaQuery,
  useTheme,
} from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";
import SearchIcon from "@mui/icons-material/Search";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import DownloadIcon from "@mui/icons-material/Download";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import { stripAnsi } from "../Utils.ts";

interface ViewOutputDialogProps {
  recordType: string;
  recordId: number;
  computeHistoryId: number | undefined;
  initialKey?: string;
  open: boolean;
  onClose: () => void;
}

interface ViewOutputButtonProps {
  recordType: string;
  recordId: number;
  computeHistoryId: number | undefined;
}

function getPlainText(data: unknown): string {
  if (typeof data === "string") return stripAnsi(data);
  if (data && typeof data === "object") {
    return Object.entries(data)
      .map(([k, v]) => `${k}: ${typeof v === "string" ? v : JSON.stringify(v, null, 2)}`)
      .join("\n");
  }
  return String(data);
}

interface HighlightedTextProps {
  text: string;
  search: string;
  currentMatchIndex: number;
  matchRefs: React.RefObject<(HTMLElement | null)[]>;
}

const HighlightedText: React.FC<HighlightedTextProps> = ({
  text,
  search,
  currentMatchIndex,
  matchRefs,
}) => {
  if (!search) return <>{text}</>;

  const parts: React.ReactNode[] = [];
  const lowerText = text.toLowerCase();
  const lowerSearch = search.toLowerCase();
  let lastIndex = 0;
  let matchIndex = 0;

  let pos = lowerText.indexOf(lowerSearch, lastIndex);
  while (pos !== -1) {
    if (pos > lastIndex) {
      parts.push(text.slice(lastIndex, pos));
    }
    const isActive = matchIndex === currentMatchIndex;
    const idx = matchIndex;
    parts.push(
      <mark
        key={`m-${pos}`}
        ref={(el) => { matchRefs.current[idx] = el; }}
        style={{
          backgroundColor: isActive ? "#ff9632" : "#fff176",
          color: "inherit",
          borderRadius: 2,
          padding: "0 1px",
        }}
      >
        {text.slice(pos, pos + search.length)}
      </mark>,
    );
    matchIndex++;
    lastIndex = pos + search.length;
    pos = lowerText.indexOf(lowerSearch, lastIndex);
  }

  if (lastIndex < text.length) {
    parts.push(text.slice(lastIndex));
  }

  return <>{parts}</>;
};


export const ViewOutputDialog: React.FC<ViewOutputDialogProps> = ({
  recordType,
  recordId,
  computeHistoryId,
  initialKey,
  open,
  onClose,
}) => {
  const [selectedKey, setSelectedKey] = useState<string | null>(
    initialKey || null,
  );
  const [searchTerm, setSearchTerm] = useState("");
  const [currentMatchIndex, setCurrentMatchIndex] = useState(0);
  const [copyTooltip, setCopyTooltip] = useState("Copy to clipboard");
  const matchRefs = useRef<(HTMLElement | null)[]>([]);

  const theme = useTheme();
  const fullScreen = useMediaQuery(theme.breakpoints.down("md"));
  const { makeRequest } = usePortalClient();

  const {
    status: historyStatus,
    data: historyData,
    error: historyError,
  } = useQuery({
    queryKey: ["recordComputeHistory", recordId],
    queryFn: () =>
      makeRequest<qcpTypes.ComputeHistory[]>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history`,
      ),
    enabled: open && computeHistoryId === undefined,
  });

  const effectiveComputeHistoryId =
    computeHistoryId ??
    (historyData && historyData.length > 0
      ? historyData[historyData.length - 1].id
      : undefined);

  const {
    status: outputKeysStatus,
    data: outputKeysData,
    error: outputKeysError,
  } = useQuery({
    queryKey: [
      "recordOutputs",
      recordType,
      recordId,
      effectiveComputeHistoryId,
    ],
    queryFn: () =>
      makeRequest<Record<string, any>>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${effectiveComputeHistoryId}/outputs`,
      ),
    enabled: open && effectiveComputeHistoryId !== undefined,
  });

  const outputKeys = outputKeysData ? Object.keys(outputKeysData) : [];

  const effectiveKey =
    selectedKey !== null && outputKeys.includes(selectedKey)
      ? selectedKey
      : outputKeys.length > 0
        ? outputKeys[0]
        : null;

  const {
    status: outputContentStatus,
    data: outputContentData,
    error: outputContentError,
  } = useQuery({
    queryKey: [
      "recordOutputContent",
      recordType,
      recordId,
      effectiveComputeHistoryId,
      effectiveKey,
    ],
    queryFn: () =>
      makeRequest<string>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${effectiveComputeHistoryId}/outputs/${effectiveKey}/uncompressed_data`,
      ),
    enabled: open && effectiveComputeHistoryId !== undefined && !!effectiveKey,
  });

  const plainText = useMemo(
    () => (outputContentData ? getPlainText(outputContentData) : ""),
    [outputContentData],
  );

  const totalMatches = useMemo(() => {
    if (!searchTerm || !plainText) return 0;
    const lowerText = plainText.toLowerCase();
    const lowerSearch = searchTerm.toLowerCase();
    let count = 0;
    let pos = lowerText.indexOf(lowerSearch);
    while (pos !== -1) {
      count++;
      pos = lowerText.indexOf(lowerSearch, pos + lowerSearch.length);
    }
    return count;
  }, [plainText, searchTerm]);

  useEffect(() => {
    setCurrentMatchIndex(0);
    matchRefs.current = [];
  }, [searchTerm]);

  useEffect(() => {
    if (totalMatches > 0 && matchRefs.current[currentMatchIndex]) {
      matchRefs.current[currentMatchIndex]?.scrollIntoView({
        behavior: "smooth",
        block: "center",
      });
    }
  }, [currentMatchIndex, totalMatches]);

  const handleClose = useCallback(() => {
    setSelectedKey(initialKey || null);
    setSearchTerm("");
    setCurrentMatchIndex(0);
    onClose();
  }, [initialKey, onClose]);

  const handleCopy = useCallback(async () => {
    if (!plainText) return;
    await navigator.clipboard.writeText(plainText);
    setCopyTooltip("Copied!");
    setTimeout(() => setCopyTooltip("Copy to clipboard"), 1500);
  }, [plainText]);

  const handleDownload = useCallback(() => {
    if (!plainText || !effectiveKey) return;
    const blob = new Blob([plainText], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${recordId}_${effectiveKey}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  }, [plainText, effectiveKey, recordId]);

  const handlePrevMatch = useCallback(() => {
    setCurrentMatchIndex((prev) =>
      totalMatches === 0 ? 0 : (prev - 1 + totalMatches) % totalMatches,
    );
  }, [totalMatches]);

  const handleNextMatch = useCallback(() => {
    setCurrentMatchIndex((prev) =>
      totalMatches === 0 ? 0 : (prev + 1) % totalMatches,
    );
  }, [totalMatches]);

  const handleSearchKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter") {
        e.preventDefault();
        if (e.shiftKey) handlePrevMatch();
        else handleNextMatch();
      }
    },
    [handleNextMatch, handlePrevMatch],
  );

  const isLoading =
    (computeHistoryId === undefined && historyStatus === "pending") ||
    (effectiveComputeHistoryId !== undefined && outputKeysStatus === "pending");

  const hasContent =
    outputKeysStatus === "success" && outputKeysData && outputKeys.length > 0;

  return (
    <Dialog
      fullWidth
      maxWidth="lg"
      fullScreen={fullScreen}
      open={open}
      onClose={handleClose}
      onClick={(e) => e.stopPropagation()}
    >
      <DialogContent
        sx={{
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          p: 0,
          height: fullScreen ? "100vh" : "80vh",
        }}
      >
        {/* Toolbar */}
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            gap: 1,
            px: 2,
            py: 1,
            borderBottom: 1,
            borderColor: "divider",
            flexShrink: 0,
          }}
        >
          <TextField
            size="small"
            placeholder="Search output..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            onKeyDown={handleSearchKeyDown}
            disabled={!hasContent || outputContentStatus !== "success"}
            slotProps={{
              input: {
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                ),
              },
            }}
            sx={{ minWidth: 200, maxWidth: 350 }}
          />
          {searchTerm && totalMatches > 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ whiteSpace: "nowrap" }}>
              {currentMatchIndex + 1} / {totalMatches}
            </Typography>
          )}
          {searchTerm && totalMatches === 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ whiteSpace: "nowrap" }}>
              No matches
            </Typography>
          )}
          {searchTerm && totalMatches > 0 && (
            <>
              <IconButton size="small" onClick={handlePrevMatch} aria-label="Previous match">
                <KeyboardArrowUpIcon fontSize="small" />
              </IconButton>
              <IconButton size="small" onClick={handleNextMatch} aria-label="Next match">
                <KeyboardArrowDownIcon fontSize="small" />
              </IconButton>
            </>
          )}

          <Box sx={{ flex: 1 }} />

          <Tooltip title={copyTooltip}>
            <span>
              <IconButton
                size="small"
                onClick={handleCopy}
                disabled={!plainText}
                aria-label="Copy to clipboard"
              >
                <ContentCopyIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Download as text file">
            <span>
              <IconButton
                size="small"
                onClick={handleDownload}
                disabled={!plainText}
                aria-label="Download output"
              >
                <DownloadIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <IconButton size="small" onClick={handleClose} aria-label="Close">
            <CloseIcon fontSize="small" />
          </IconButton>
        </Box>

        {/* Body */}
        <Box sx={{ flex: 1, display: "flex", overflow: "hidden" }}>
          {isLoading && (
            <Box sx={{ p: 3, width: "100%" }}>
              <LoadingIndicator />
            </Box>
          )}

          {historyStatus === "error" && (
            <Box sx={{ p: 3 }}>
              <ErrorIndicator message={(historyError as any).message} />
            </Box>
          )}

          {outputKeysStatus === "error" && (
            <Box sx={{ p: 3 }}>
              <ErrorIndicator message={(outputKeysError as any).message} />
            </Box>
          )}

          {computeHistoryId === undefined &&
            historyStatus === "success" &&
            historyData &&
            historyData.length === 0 && (
              <Box sx={{ p: 3 }}>
                <Typography>No compute history found for this record.</Typography>
              </Box>
            )}

          {outputKeysStatus === "success" &&
            outputKeysData &&
            outputKeys.length === 0 && (
              <Box sx={{ p: 3 }}>
                <Typography>No outputs found for this compute history.</Typography>
              </Box>
            )}

          {hasContent && (
            <Box
              sx={{
                flex: 1,
                display: "flex",
                flexDirection: fullScreen ? "column" : "row",
                overflow: "hidden",
              }}
            >
              {/* Tabs */}
              <Tabs
                orientation={fullScreen ? "horizontal" : "vertical"}
                variant="scrollable"
                scrollButtons="auto"
                value={effectiveKey || false}
                onChange={(_event, newValue) => {
                  setSelectedKey(newValue);
                  setSearchTerm("");
                }}
                sx={{
                  flexShrink: 0,
                  ...(fullScreen
                    ? { borderBottom: 1, borderColor: "divider" }
                    : { borderRight: 1, borderColor: "divider", minWidth: 140 }),
                }}
              >
                {outputKeys.map((key) => (
                  <Tab key={key} label={key.toUpperCase()} value={key} />
                ))}
              </Tabs>

              {/* Output content */}
              <Box
                sx={{
                  flex: 1,
                  overflow: "auto",
                  p: 2,
                }}
              >
                {outputContentStatus === "pending" && <LoadingIndicator />}
                {outputContentStatus === "error" && (
                  <ErrorIndicator message={(outputContentError as any).message} />
                )}
                {outputContentStatus === "success" && outputContentData && (
                  <Box
                    sx={{
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                      fontFamily: "monospace",
                      fontSize: "0.85rem",
                      lineHeight: 1.5,
                    }}
                  >
                    <HighlightedText
                      text={plainText}
                      search={searchTerm}
                      currentMatchIndex={currentMatchIndex}
                      matchRefs={matchRefs}
                    />
                  </Box>
                )}
              </Box>
            </Box>
          )}
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export const ViewOutputButton: React.FC<ViewOutputButtonProps> = ({
  recordType,
  recordId,
  computeHistoryId,
}) => {
  const [open, setOpen] = useState(false);

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        onClick={(e) => {
          e.stopPropagation();
          setOpen(true);
        }}
      >
        View Output
      </Button>

      <ViewOutputDialog
        recordType={recordType}
        recordId={recordId}
        computeHistoryId={computeHistoryId}
        open={open}
        onClose={() => {
          setOpen(false);
        }}
      />
    </>
  );
};
