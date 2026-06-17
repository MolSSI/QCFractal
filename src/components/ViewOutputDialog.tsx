import React, {
  useState,
  useCallback,
  useMemo,
  useRef,
  useEffect,
  useDeferredValue,
} from "react";
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
import VerticalAlignTopIcon from "@mui/icons-material/VerticalAlignTop";
import VerticalAlignBottomIcon from "@mui/icons-material/VerticalAlignBottom";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import { stripAnsi } from "../Utils.ts";

// Line height must match: font-size 0.85rem (~13.6px) × line-height 1.5 ≈ 20.4px → round up
const LINE_HEIGHT = 21;
// Extra lines to render above/below viewport to avoid visible blank flashes on fast scroll
const RENDER_BUFFER = 20;

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

interface MatchPosition {
  lineIndex: number;
  charOffset: number;
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

interface HighlightedLineProps {
  line: string;
  lineIndex: number;
  search: string;
  lineMatches: MatchPosition[];
  currentMatchIndex: number;
  globalMatchOffset: number;
}

const HighlightedLine: React.FC<HighlightedLineProps> = ({
  line,
  lineMatches,
  search,
  currentMatchIndex,
  globalMatchOffset,
}) => {
  if (!search || lineMatches.length === 0) {
    return <>{line || "​"}</>;
  }

  const parts: React.ReactNode[] = [];
  let lastChar = 0;

  lineMatches.forEach((match, i) => {
    const globalIdx = globalMatchOffset + i;
    const start = match.charOffset;
    const end = start + search.length;
    if (start > lastChar) {
      parts.push(line.slice(lastChar, start));
    }
    parts.push(
      <mark
        key={`${match.lineIndex}-${start}`}
        style={{
          backgroundColor: globalIdx === currentMatchIndex ? "#ff9632" : "#fff176",
          color: "inherit",
          borderRadius: 2,
          padding: "0 1px",
        }}
      >
        {line.slice(start, end)}
      </mark>,
    );
    lastChar = end;
  });

  if (lastChar < line.length) {
    parts.push(line.slice(lastChar));
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
  const [selectedKey, setSelectedKey] = useState<string | null>(initialKey || null);
  const [searchInputValue, setSearchInputValue] = useState("");
  const [currentMatchIndex, setCurrentMatchIndex] = useState(0);
  const [copyTooltip, setCopyTooltip] = useState("Copy to clipboard");
  const [scrollTop, setScrollTop] = useState(0);
  const [containerHeight, setContainerHeight] = useState(600);

  const scrollContainerRef = useRef<HTMLDivElement>(null);

  // Two-stage search deferral: 200ms debounce prevents the match scan from
  // firing on every keystroke; useDeferredValue keeps the UI responsive even
  // during the (rare) case where the scan itself takes a full frame.
  const [debouncedSearch, setDebouncedSearch] = useState("");
  useEffect(() => {
    const id = setTimeout(() => setDebouncedSearch(searchInputValue), 200);
    return () => clearTimeout(id);
  }, [searchInputValue]);
  const deferredSearch = useDeferredValue(debouncedSearch);

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
    queryKey: ["recordOutputs", recordType, recordId, effectiveComputeHistoryId],
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

  const lines = useMemo(() => plainText.split("\n"), [plainText]);

  // Longest line drives a stable content width so the horizontal scrollbar
  // reflects the full output rather than only the currently visible lines.
  const maxLineLength = useMemo(
    () => lines.reduce((max, line) => Math.max(max, line.length), 0),
    [lines],
  );

  // Pre-compute all match positions by line using the deferred search value.
  // Runs only when deferredSearch changes (not on every keystroke).
  const matchPositions = useMemo((): MatchPosition[] => {
    if (!deferredSearch || !lines.length) return [];
    const lowerSearch = deferredSearch.toLowerCase();
    const result: MatchPosition[] = [];
    for (let lineIndex = 0; lineIndex < lines.length; lineIndex++) {
      const lowerLine = lines[lineIndex].toLowerCase();
      let pos = lowerLine.indexOf(lowerSearch);
      while (pos !== -1) {
        result.push({ lineIndex, charOffset: pos });
        pos = lowerLine.indexOf(lowerSearch, pos + lowerSearch.length);
      }
    }
    return result;
  }, [lines, deferredSearch]);

  const totalMatches = matchPositions.length;

  // Group match positions by line index for O(1) lookup during render
  const matchesByLine = useMemo((): Map<number, { matches: MatchPosition[]; globalOffset: number }> => {
    const map = new Map<number, { matches: MatchPosition[]; globalOffset: number }>();
    for (let i = 0; i < matchPositions.length; i++) {
      const { lineIndex } = matchPositions[i];
      if (!map.has(lineIndex)) {
        map.set(lineIndex, { matches: [], globalOffset: i });
      }
      map.get(lineIndex)!.matches.push(matchPositions[i]);
    }
    return map;
  }, [matchPositions]);

  useEffect(() => {
    setCurrentMatchIndex(0);
  }, [deferredSearch]);

  // Measure container height when dialog opens or the selected key changes
  useEffect(() => {
    if (scrollContainerRef.current) {
      setContainerHeight(scrollContainerRef.current.clientHeight);
    }
  }, [open, effectiveKey]);

  // Focus the output container once its content loads so arrow/page keys scroll
  // immediately without requiring a click first.
  useEffect(() => {
    if (outputContentStatus === "success" && scrollContainerRef.current) {
      scrollContainerRef.current.focus({ preventScroll: true });
    }
  }, [outputContentStatus, effectiveKey]);

  // Scroll the virtual container so the current match is centered in the viewport.
  // No DOM refs to marks needed — we know exactly which line the match is on.
  useEffect(() => {
    if (totalMatches === 0 || !scrollContainerRef.current) return;
    const targetLine = matchPositions[currentMatchIndex]?.lineIndex;
    if (targetLine === undefined) return;
    const targetScrollTop = targetLine * LINE_HEIGHT - containerHeight / 2;
    scrollContainerRef.current.scrollTop = Math.max(0, targetScrollTop);
  }, [currentMatchIndex, totalMatches, matchPositions, containerHeight]);

  // Virtual scroll: compute the visible line range
  const firstLine = Math.max(0, Math.floor(scrollTop / LINE_HEIGHT) - RENDER_BUFFER);
  const lastLine = Math.min(
    lines.length - 1,
    Math.ceil((scrollTop + containerHeight) / LINE_HEIGHT) + RENDER_BUFFER,
  );

  const handleClose = useCallback(() => {
    setSelectedKey(initialKey || null);
    setSearchInputValue("");
    setCurrentMatchIndex(0);
    setScrollTop(0);
    onClose();
  }, [initialKey, onClose]);

  const handleScrollToTop = useCallback(() => {
    scrollContainerRef.current?.scrollTo({ top: 0, behavior: "smooth" });
  }, []);

  const handleScrollToBottom = useCallback(() => {
    const el = scrollContainerRef.current;
    if (el) el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
  }, []);

  // Keyboard scrolling for the output container. A plain scrollable <div> only
  // responds to arrow keys when it holds focus, and even then the virtualized
  // content can confuse native key handling — so we move scrollTop explicitly.
  const handleContainerKeyDown = useCallback((e: React.KeyboardEvent<HTMLDivElement>) => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const page = el.clientHeight * 0.9;
    let delta: number | null = null;
    switch (e.key) {
      case "ArrowDown":
        delta = LINE_HEIGHT * 3;
        break;
      case "ArrowUp":
        delta = -LINE_HEIGHT * 3;
        break;
      case "PageDown":
        delta = page;
        break;
      case "PageUp":
        delta = -page;
        break;
      case "Home":
        e.preventDefault();
        el.scrollTo({ top: 0 });
        return;
      case "End":
        e.preventDefault();
        el.scrollTo({ top: el.scrollHeight });
        return;
      default:
        return;
    }
    e.preventDefault();
    el.scrollTop += delta;
  }, []);

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
            value={searchInputValue}
            onChange={(e) => setSearchInputValue(e.target.value)}
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
          {deferredSearch && totalMatches > 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ whiteSpace: "nowrap" }}>
              {currentMatchIndex + 1} / {totalMatches}
            </Typography>
          )}
          {deferredSearch && totalMatches === 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ whiteSpace: "nowrap" }}>
              No matches
            </Typography>
          )}
          {deferredSearch && totalMatches > 0 && (
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
                  setSearchInputValue("");
                  setScrollTop(0);
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

              {/* Virtual scroll output container */}
              <Box sx={{ flex: 1, position: "relative", display: "flex", overflow: "hidden" }}>
              <Box
                ref={scrollContainerRef}
                tabIndex={0}
                onScroll={(e) => setScrollTop(e.currentTarget.scrollTop)}
                onKeyDown={handleContainerKeyDown}
                sx={{
                  flex: 1,
                  overflow: "auto",
                  position: "relative",
                  outline: "none",
                  // Force always-visible, grabbable scrollbars. macOS overlay
                  // scrollbars are hidden until you actively scroll; styling the
                  // pseudo-elements opts into persistent classic scrollbars so the
                  // horizontal bar is discoverable when lines are wider than the view.
                  "&::-webkit-scrollbar": { width: 12, height: 12 },
                  "&::-webkit-scrollbar-thumb": {
                    backgroundColor: "rgba(128,128,128,0.5)",
                    borderRadius: "6px",
                    border: "2px solid transparent",
                    backgroundClip: "content-box",
                  },
                  "&::-webkit-scrollbar-thumb:hover": {
                    backgroundColor: "rgba(128,128,128,0.8)",
                  },
                  "&::-webkit-scrollbar-corner": { backgroundColor: "transparent" },
                }}
              >
                {outputContentStatus === "pending" && <LoadingIndicator />}
                {outputContentStatus === "error" && (
                  <ErrorIndicator message={(outputContentError as any).message} />
                )}
                {outputContentStatus === "success" && outputContentData && (
                  // Full-height spacer keeps the scrollbar proportional to total content
                  <Box sx={{ height: lines.length * LINE_HEIGHT, position: "relative" }}>
                    {/* Absolutely positioned so it "floats" at the correct scroll offset */}
                    <Box
                      sx={{
                        position: "absolute",
                        top: firstLine * LINE_HEIGHT,
                        left: 0,
                        px: 2,
                        boxSizing: "border-box",
                        fontFamily: "monospace",
                        fontSize: "0.85rem",
                        // Width tracks the longest line (in monospace `ch` units) so
                        // the container's scrollWidth exceeds its width only when the
                        // content is actually wider — showing the scrollbar on demand.
                        minWidth: "100%",
                        width: `calc(${maxLineLength}ch + 32px)`,
                      }}
                    >
                      {lines.slice(firstLine, lastLine + 1).map((line, i) => {
                        const globalLineIndex = firstLine + i;
                        const lineMatchInfo = matchesByLine.get(globalLineIndex);
                        return (
                          <Box
                            key={globalLineIndex}
                            sx={{
                              height: LINE_HEIGHT,
                              fontFamily: "monospace",
                              fontSize: "0.85rem",
                              lineHeight: `${LINE_HEIGHT}px`,
                              whiteSpace: "pre",
                              overflow: "visible",
                            }}
                          >
                            <HighlightedLine
                              line={line}
                              lineIndex={globalLineIndex}
                              search={deferredSearch}
                              lineMatches={lineMatchInfo?.matches ?? []}
                              currentMatchIndex={currentMatchIndex}
                              globalMatchOffset={lineMatchInfo?.globalOffset ?? 0}
                            />
                          </Box>
                        );
                      })}
                    </Box>
                  </Box>
                )}
              </Box>

              {/* Floating scroll-to-top / scroll-to-bottom buttons */}
              {outputContentStatus === "success" && outputContentData && (
                <Box
                  sx={{
                    position: "absolute",
                    bottom: 16,
                    right: 16,
                    display: "flex",
                    flexDirection: "column",
                    gap: 1,
                  }}
                >
                  <Tooltip title="Scroll to top" placement="left">
                    <IconButton
                      size="small"
                      onClick={handleScrollToTop}
                      aria-label="Scroll to top"
                      sx={{
                        bgcolor: "background.paper",
                        boxShadow: 2,
                        "&:hover": { bgcolor: "action.hover" },
                      }}
                    >
                      <VerticalAlignTopIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Scroll to bottom" placement="left">
                    <IconButton
                      size="small"
                      onClick={handleScrollToBottom}
                      aria-label="Scroll to bottom"
                      sx={{
                        bgcolor: "background.paper",
                        boxShadow: 2,
                        "&:hover": { bgcolor: "action.hover" },
                      }}
                    >
                      <VerticalAlignBottomIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
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
