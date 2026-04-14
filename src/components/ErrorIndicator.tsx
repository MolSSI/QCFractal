import { Box, Typography } from "@mui/material";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";

interface ErrorIndicatorProps {
  message?: string;
  fullPage?: boolean;
}

function ErrorIndicator({ message = "An error occurred.", fullPage = false }: ErrorIndicatorProps) {
  const content = (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      gap={2}
      sx={{ py: 6 }}
    >
      <ErrorOutlineIcon color="error" sx={{ fontSize: 48 }} />
      <Typography variant="body2" color="error" fontWeight={1000}>
        {message}
      </Typography>
    </Box>
  );

  if (fullPage) {
    return (
      <Box
        display="flex"
        alignItems="center"
        justifyContent="center"
        sx={{ minHeight: "70vh", width: "100%" }}
      >
        {content}
      </Box>
    );
  }

  return content;
}

export default ErrorIndicator;
