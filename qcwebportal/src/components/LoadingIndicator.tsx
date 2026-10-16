import { Box, CircularProgress, Typography } from "@mui/material";

interface LoadingIndicatorProps {
  message?: string;
  fullPage?: boolean;
}

function LoadingIndicator({ message = "Loading...", fullPage = false }: LoadingIndicatorProps) {
  const content = (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      gap={2}
      sx={{ py: 6 }}
    >
      <CircularProgress size={48} />
      <Typography variant="body2" color="text.secondary">
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

export default LoadingIndicator;
