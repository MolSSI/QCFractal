import React from "react";
import { Typography, List, ListItem, ListItemText, Box } from "@mui/material";
import { RecordTask, RecordService } from "../PortalTypes.ts";

function renderValue(value: any): React.ReactNode {
  if (Array.isArray(value)) {
    return value.length > 0 ? value.join(", ") : "None";
  }
  if (value === null || value === undefined || value === "") {
    return "None";
  }
  if (typeof value === "object") {
    return (
      <List dense sx={{ pl: 2 }}>
        {Object.entries(value)
          .filter(([k]) => k !== "function_kwargs_compressed")
          .map(([k, v]) => (
            <ListItem key={k} disablePadding>
              <ListItemText
                primary={
                  <>
                    <strong>{k}:</strong> {renderValue(v)}
                  </>
                }
              />
            </ListItem>
          ))}
      </List>
    );
  }
  return String(value);
}

const TaskServiceFragment: React.FC<{
  data: RecordService | RecordTask | undefined;
  type: "task" | "service";
}> = ({ data, type }) => {
  if (!data) {
    return <Typography>No {type} data available.</Typography>;
  }

  return (
    <Box>
      <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
        {type.charAt(0).toUpperCase() + type.slice(1)} Details
      </Typography>
      <List dense>
        {Object.entries(data)
          .filter(([key]) => key !== "function_kwargs_compressed")
          .map(([key, value]) => (
            <ListItem key={key} alignItems="flex-start" disablePadding>
              <ListItemText
                primary={
                  <>
                    <strong>{key}:</strong> {renderValue(value)}
                  </>
                }
              />
            </ListItem>
          ))}
      </List>
    </Box>
  );
};

export default TaskServiceFragment;
