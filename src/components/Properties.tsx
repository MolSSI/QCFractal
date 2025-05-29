import React from "react";
import { Typography, List, ListItem, ListItemText, Box } from "@mui/material";

interface PropertiesProps {
  properties: Record<string, any>;
}

const renderValue = (value: any) => {
  if (Array.isArray(value)) {
    return value.length > 0 ? value.join(", ") : "None";
  }
  if (value === null || value === undefined || value === "") {
    return "None";
  }
  if (typeof value === "object") {
    return (
      <List dense sx={{ pl: 2 }}>
        {Object.entries(value).map(([k, v]) => (
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
};

const Properties: React.FC<PropertiesProps> = ({ properties }) => (
  <Box sx={{ p: 2, height: "100%" }}>
    <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
      Properties
    </Typography>
    {properties && Object.keys(properties).length > 0 ? (
      <List dense>
        {Object.entries(properties).map(([key, value]) => (
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
    ) : (
      <Typography>No properties for this record</Typography>
    )}
  </Box>
);

export default Properties;
