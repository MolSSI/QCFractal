import React from "react";
import JsonView, { type SectionElement } from "@uiw/react-json-view";
import { Box, Tooltip } from "@mui/material";
import { useTheme } from "@mui/material/styles";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import CheckIcon from "@mui/icons-material/Check";

const LONG_ARRAY_LENGTH = 10;
const SHORTEN_TEXT_LENGTH = 60;

const renderCopyButton: SectionElement<"svg">["render"] = (
  props,
  { value },
) => {
  const copied = (props as { "data-copied"?: boolean })["data-copied"];
  const label = copied
    ? "Copied"
    : typeof value === "object" && value !== null
      ? "Copy as JSON"
      : "Copy value";

  return (
    <Tooltip title={label} placement="top" disableInteractive>
      <Box
        component="span"
        role="button"
        aria-label={label}
        onClick={props.onClick as React.MouseEventHandler}
        sx={{
          display: "inline-flex",
          alignItems: "center",
          ml: 0.75,
          cursor: "pointer",
          color: copied ? "success.main" : "text.secondary",
          "&:hover": { color: copied ? "success.main" : "text.primary" },
        }}
      >
        {copied ? (
          <CheckIcon sx={{ fontSize: 14 }} />
        ) : (
          <ContentCopyIcon sx={{ fontSize: 14 }} />
        )}
      </Box>
    </Tooltip>
  );
};

const renderLongString: React.ComponentProps<
  typeof JsonView.String
>["render"] = (props, { type, value }) => {
  if (type !== "value" || String(value).length <= SHORTEN_TEXT_LENGTH) {
    return undefined;
  }
  const shortened = props.className?.includes("w-rjv-value-short");
  const quote = (
    <span style={{ color: "var(--w-rjv-quotes-string-color)" }}>"</span>
  );

  return (
    <>
      {quote}
      <Tooltip
        title={shortened ? "Click to show full text" : "Click to collapse"}
        placement="top"
        followCursor
        disableInteractive
      >
        <span {...props} />
      </Tooltip>
      {quote}
    </>
  );
};

const copyStringsUnquoted = (
  copyText: string,
  _keyName?: string | number,
  value?: unknown,
) => (typeof value === "string" ? value : copyText);

export interface JsonViewerProps {
  value: object;
  expandDepth?: number;
  sortKeys?: boolean;
}

export const JsonViewer: React.FC<JsonViewerProps> = ({
  value,
  expandDepth = 2,
  sortKeys = false,
}) => {
  const theme = useTheme();
  const palette = (theme.vars ?? theme).palette;

  const style = {
    "--w-rjv-font-family": "Menlo, Consolas, monospace",
    "--w-rjv-color": palette.text.primary,
    "--w-rjv-key-string": palette.text.primary,
    "--w-rjv-background-color": "transparent",
    "--w-rjv-line-color": palette.divider,
    "--w-rjv-arrow-color": palette.text.secondary,
    "--w-rjv-info-color": palette.text.secondary,
    "--w-rjv-copied-color": palette.text.secondary,
    "--w-rjv-copied-success-color": palette.success.main,
    "--w-rjv-colon-color": palette.text.secondary,
    "--w-rjv-curlybraces-color": palette.text.secondary,
    "--w-rjv-brackets-color": palette.text.secondary,
    "--w-rjv-ellipsis-color": palette.text.secondary,
    "--w-rjv-type-string-color": palette.warning.main,
    "--w-rjv-quotes-string-color": palette.warning.main,
    "--w-rjv-type-int-color": palette.primary.main,
    "--w-rjv-type-float-color": palette.primary.main,
    "--w-rjv-type-bigint-color": palette.primary.main,
    "--w-rjv-type-boolean-color": palette.success.main,
    "--w-rjv-type-null-color": palette.text.secondary,
    "--w-rjv-type-undefined-color": palette.text.secondary,
    fontSize: 13,
    textIndent: 0,
    whiteSpace: "pre",
  } as React.CSSProperties;

  return (
    <Box sx={{ overflowX: "auto" }}>
      <JsonView
        value={value}
        shouldExpandNodeInitially={(_, { level, value: node }) =>
          level <= expandDepth &&
          !(Array.isArray(node) && node.length > LONG_ARRAY_LENGTH)
        }
        objectSortKeys={sortKeys}
        displayDataTypes={false}
        shortenTextAfterLength={SHORTEN_TEXT_LENGTH}
        highlightUpdates={false}
        beforeCopy={copyStringsUnquoted}
        style={style}
      >
        <JsonView.Quote render={() => <span />} />
        <JsonView.Copied render={renderCopyButton} />
        <JsonView.String render={renderLongString} />
      </JsonView>
    </Box>
  );
};

export default JsonViewer;
