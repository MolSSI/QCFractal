import React from "react";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import {
  Box,
  Divider,
  IconButton,
  Link,
  Paper,
  Stack,
  Tooltip,
  Typography,
  useTheme,
} from "@mui/material";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import {
  oneDark,
  oneLight,
} from "react-syntax-highlighter/dist/esm/styles/prism";
import { usePageTitle } from "../UsePageTitle.ts";
import { server_address } from "../request_config.ts";
import { useResolvedColorMode } from "../shared-theme/useResolvedColorMode.ts";

interface CopyableCodeBlockProps {
  code: string;
  language: string;
}

function CopyableCodeBlock({ code, language }: CopyableCodeBlockProps) {
  const theme = useTheme();
  const colorMode = useResolvedColorMode();
  const [copyTooltip, setCopyTooltip] = React.useState("Copy to clipboard");
  const timeoutRef = React.useRef<number | null>(null);

  React.useEffect(() => {
    return () => {
      if (timeoutRef.current !== null) {
        window.clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const handleCopy = React.useCallback(async () => {
    await navigator.clipboard.writeText(code);
    setCopyTooltip("Copied!");

    if (timeoutRef.current !== null) {
      window.clearTimeout(timeoutRef.current);
    }

    timeoutRef.current = window.setTimeout(() => {
      setCopyTooltip("Copy to clipboard");
      timeoutRef.current = null;
    }, 1500);
  }, [code]);

  const syntaxTheme = colorMode === "dark" ? oneDark : oneLight;

  return (
    <Box
      sx={{
        border: 1,
        borderColor: "divider",
        borderRadius: 1,
        overflow: "hidden",
        position: "relative",
      }}
    >
      <Box sx={{ position: "absolute", top: 8, right: 8, zIndex: 1 }}>
        <Tooltip title={copyTooltip}>
          <IconButton
            size="small"
            onClick={handleCopy}
            aria-label="Copy code to clipboard"
            sx={{
              color: colorMode === "dark" ? "grey.300" : "grey.700",
              backgroundColor:
                colorMode === "dark"
                  ? "rgba(255, 255, 255, 0.1)"
                  : "rgba(0, 0, 0, 0.05)",
              "&:hover": {
                backgroundColor:
                  colorMode === "dark"
                    ? "rgba(255, 255, 255, 0.2)"
                    : "rgba(0, 0, 0, 0.1)",
              },
            }}
          >
            <ContentCopyIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Box>
      <SyntaxHighlighter
        language={language}
        style={syntaxTheme}
        wrapLongLines
        customStyle={{
          margin: 0,
          padding: theme.spacing(2),
          paddingRight: theme.spacing(6),
          fontSize: "0.9rem",
        }}
      >
        {code}
      </SyntaxHighlighter>
    </Box>
  );
}

const installWithPip = `python -m pip install qcportal`;

const installWithConda = `conda create -n qcportal qcportal -c conda-forge
conda activate qcportal`;

const pythonCode = `from qcportal import PortalClient

client = PortalClient("${server_address}", username="user", password="pass")`;

const APIInfo: React.FC = () => {
  usePageTitle("API Access");

  return (
    <Box width="100%" sx={{ p: 2 }}>
      <Typography variant="h4" gutterBottom>
        API Access
      </Typography>
      <Typography variant="body1" sx={{ mb: 3 }}>
        The QCFractal API provides programmatic access to quantum chemistry
        data and compute capabilities through the QCPortal Python client.
      </Typography>

      <Stack spacing={4}>
        <Paper sx={{ p: 3 }}>
          <Typography variant="h5" gutterBottom>
            Installation
          </Typography>
          <Divider sx={{ mb: 2 }} />
          <Typography variant="body1" component="p" sx={{ mb: 2 }}>
            Install <code>qcportal</code> into a dedicated Python environment.
            If you already manage your own environment, <code>pip</code> is the
            quickest option.
          </Typography>
          <Stack spacing={2.5}>
            <Box>
              <Typography variant="subtitle1" sx={{ mb: 1, fontWeight: 600 }}>
                pip
              </Typography>
              <CopyableCodeBlock code={installWithPip} language="bash" />
            </Box>
            <Box>
              <Typography variant="subtitle1" sx={{ mb: 1, fontWeight: 600 }}>
                conda / mamba
              </Typography>
              <CopyableCodeBlock code={installWithConda} language="bash" />
            </Box>
          </Stack>
        </Paper>

        <Paper sx={{ p: 3 }}>
          <Typography variant="h5" gutterBottom>
            Connect From Python
          </Typography>
          <Divider sx={{ mb: 2 }} />
          <Typography variant="body1" component="p" sx={{ mb: 2 }}>
            After installation, create a <code>PortalClient</code> with this
            server address and your QCFractal credentials:
          </Typography>
          <CopyableCodeBlock code={pythonCode} language="python" />
          <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
            Use a username and password with permission to access the records or
            actions you need. For repeated use, you can also move connection
            settings into a QCPortal configuration file or environment
            variables.
          </Typography>
        </Paper>

        <Paper sx={{ p: 3 }}>
          <Typography variant="h5" gutterBottom>
            Documentation
          </Typography>
          <Divider sx={{ mb: 2 }} />
          <Typography variant="body1" component="p" sx={{ mb: 1.5 }}>
            The full QCArchive documentation covers installation, client usage,
            authentication, querying records, and dataset workflows.
          </Typography>
          <Link
            component="a"
            href="https://docs.qcarchive.molssi.org/"
            target="_blank"
            rel="noopener"
            variant="h6"
            sx={{ display: "inline-flex", alignItems: "center" }}
          >
            QCArchive Documentation
          </Link>
        </Paper>
      </Stack>
    </Box>
  );
};

export default APIInfo;
