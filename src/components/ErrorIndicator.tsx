import { Box, Typography } from "@mui/material";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";

interface ErrorIndicatorProps {
  message?: string;
}

function ErrorIndicator({ message = "An error occurred." }: ErrorIndicatorProps) {
  return (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      gap={2}
      sx={{ py: 6 }}
    >
      <ErrorOutlineIcon color="error" sx={{ fontSize: 48 }} />
      <Typography variant="body2" color="error">
        {message}
      </Typography>
    </Box>
  );
}

export default ErrorIndicator;
