import React from "react";
import { Chip } from "@mui/material";

interface RecordTypeProps {
  type: string;
}

export const RecordType: React.FC<RecordTypeProps> = ({ type }) => {
  return <Chip label={type} color={"default"} sx={{ fontWeight: "bold" }} />;
};

export default RecordType;
