import React from "react";
import { Typography, Box } from "@mui/material";
import JsonViewer from "./JsonViewer.tsx";

interface PropertiesProps {
  properties: Record<string, unknown>;
}

const Properties: React.FC<PropertiesProps> = ({ properties }) => (
  <Box sx={{ p: 2, height: "100%" }}>
    <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
      Properties
    </Typography>
    {properties && Object.keys(properties).length > 0 ? (
      <JsonViewer value={properties} sortKeys />
    ) : (
      <Typography>No properties for this record</Typography>
    )}
  </Box>
);

export default Properties;
