import React from "react";
import { List, ListItem, ListItemText } from "@mui/material";
import { renderValue } from "./Utils.tsx";

export interface GenericDataListEntry {
  key: string;
  label?: string;
  render?: (val: unknown) => React.ReactNode;
  showIfEmpty?: boolean;
}

export type GenericDataListKey = string | GenericDataListEntry;

export interface GenericDataListProps {
  data: Record<string, any>;
  keys: GenericDataListKey[];
}

export const GenericDataList: React.FC<GenericDataListProps> = ({ data, keys }) => {
  return (
    <List dense>
      {keys.map((k, index) => {
        const keyName = typeof k === "string" ? k : k.key;
        const label =
          typeof k === "string"
            ? k.charAt(0).toUpperCase() + k.slice(1).replace(/_/g, " ")
            : k.label ||
              k.key.charAt(0).toUpperCase() + k.key.slice(1).replace(/_/g, " ");
        const value = data[keyName];
        const customRender = typeof k === "object" ? k.render : undefined;
        const showIfEmpty = typeof k === "object" ? k.showIfEmpty ?? true : true;

        if (!showIfEmpty && (value === undefined || value === null)) {
          return null;
        }

        return (
          <ListItem key={index} disablePadding>
            <ListItemText
              primary={
                <>
                  <strong>{label}:</strong>{" "}
                  {customRender ? customRender(value) : renderValue(value)}
                </>
              }
            />
          </ListItem>
        );
      })}
    </List>
  );
};
