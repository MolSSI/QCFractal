import { Box, CircularProgress, Typography } from "@mui/material";

interface LoadingIndicatorProps {
  message?: string;
}

function LoadingIndicator({ message = "Loading..." }: LoadingIndicatorProps) {
  return (
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
}

export default LoadingIndicator;
