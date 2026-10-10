import React from "react";
import * as qcpTypes from "../PortalTypes";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Grid,
  Stack,
  Typography,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

interface ServerErrorFragmentProps {
  errorLog: qcpTypes.ServerErrorLog;
}

export const ServerErrorFragment: React.FC<ServerErrorFragmentProps> = ({
  errorLog,
}) => {
  return (
    <Box sx={{ width: "100%" }}>
      <Typography variant="h6" sx={{ mb: 2 }}>
        Server Error {errorLog.id}
      </Typography>

      <Grid container spacing={2}>
        <Grid size={12}>
          <Typography variant="subtitle2" color="text.secondary">
            Error Text
          </Typography>
          <Box
            component="pre"
            sx={{
              p: 1,
              backgroundColor: "action.hover",
              borderRadius: 1,
              overflow: "auto",
              fontSize: "0.8rem",
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
            }}
          >
            {errorLog.error_text}
          </Box>
        </Grid>

        <Grid size={12}>
          <Accordion variant="outlined">
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Typography variant="subtitle2">Request Information</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Stack spacing={2}>
                <Box>
                  <Typography variant="subtitle2" color="text.secondary">
                    Request Path
                  </Typography>
                  <Typography variant="body2">
                    {errorLog.request_path || "(none)"}
                  </Typography>
                </Box>

                <Box>
                  <Typography variant="subtitle2" color="text.secondary">
                    Request Headers
                  </Typography>
                  {!errorLog.request_headers && "(none)"}
                  {errorLog.request_headers && (
                    <Box
                      component="pre"
                      sx={{
                        p: 1,
                        backgroundColor: "action.hover",
                        borderRadius: 1,
                        overflow: "auto",
                        fontSize: "0.8rem",
                        whiteSpace: "pre-wrap",
                        wordBreak: "break-all",
                      }}
                    >
                      {JSON.stringify(errorLog.request_headers, null, 2)}
                    </Box>
                  )}
                </Box>

                <Box>
                  <Typography variant="subtitle2" color="text.secondary">
                    Request Body
                  </Typography>
                  {!errorLog.request_body && "(none)"}
                  {errorLog.request_body && (
                    <Box
                      component="pre"
                      sx={{
                        p: 1,
                        backgroundColor: "action.hover",
                        borderRadius: 1,
                        overflow: "auto",
                        fontSize: "0.8rem",
                        whiteSpace: "pre-wrap",
                        wordBreak: "break-all",
                      }}
                    >
                      {errorLog.request_body}
                    </Box>
                  )}
                </Box>
              </Stack>
            </AccordionDetails>
          </Accordion>
        </Grid>
      </Grid>
    </Box>
  );
};
