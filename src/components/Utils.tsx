import React from "react";
import { List, ListItem, ListItemText } from "@mui/material";

export const isEmpty = (value: unknown) =>
  value == null ||
  (typeof value === "object" && Object.keys(value).length === 0);

export const renderValue = (value: unknown): React.ReactNode => {
  if (Array.isArray(value)) {
    return value.length > 0 ? value.join(", ") : "None";
  }
  if (value === null || value === undefined || value === "") return "None";
  if (typeof value === "object") {
    if (isEmpty(value)) return "None";
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
};
