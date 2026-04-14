import React, { useState } from "react";
import {
  Autocomplete,
  CircularProgress,
  TextField,
  Typography,
} from "@mui/material";
import { usePortalClient } from "../PortalClient";
import * as qcpTypes from "../PortalTypes";
import { useQuery } from "@tanstack/react-query";
import RecordTypeChip from "./RecordTypeChip.tsx";

interface DatasetSearchProps {
  onDatasetSelect: (datasetId: number | null) => void;
}

export const DatasetSearch: React.FC<DatasetSearchProps> = ({
  onDatasetSelect,
}) => {
  const { makeRequest } = usePortalClient();
  const [open, setOpen] = useState(false);
  const [inputValue, setInputValue] = useState("");

  const { data: datasets = [], isLoading } = useQuery({
    queryKey: ["datasets"],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetListEntry[]>("GET", "/api/v1/datasets"),
    enabled: open,
  });

  const filteredOptions = React.useMemo(() => {
    if (!inputValue) return datasets;
    const search = inputValue.toLowerCase();
    return datasets.filter(
      (ds) =>
        ds.id.toString().includes(search) ||
        ds.dataset_name.toLowerCase().includes(search),
    );
  }, [datasets, inputValue]);

  return (
    <Autocomplete
      open={open}
      onOpen={() => setOpen(true)}
      onClose={() => setOpen(false)}
      inputValue={inputValue}
      onInputChange={(_event, newInputValue) => {
        setInputValue(newInputValue);
      }}
      onChange={(_event, newValue) => {
        onDatasetSelect(newValue ? newValue.id : null);
      }}
      isOptionEqualToValue={(option, value) => option.id === value.id}
      getOptionLabel={(option) => `[${option.id}] ${option.dataset_name}`}
      options={filteredOptions}
      loading={isLoading}
      renderInput={(params) => (
        <TextField
          {...params}
          label="Search Dataset"
          variant="outlined"
          fullWidth
          InputProps={{
            ...params.InputProps,
            endAdornment: (
              <React.Fragment>
                {isLoading ? (
                  <CircularProgress color="inherit" size={20} />
                ) : null}
                {params.InputProps.endAdornment}
              </React.Fragment>
            ),
          }}
        />
      )}
      renderOption={(props, option) => {
        const { key, ...optionProps } = props as any;
        return (
          <li key={key} {...optionProps}>
            <div>
              <Typography variant="body1">
                {option.id}: {option.dataset_name}
              </Typography>
              <Typography variant="caption" color="textSecondary">
                <RecordTypeChip type={option.dataset_type} />{" "}
                {option.tagline ? `- ${option.tagline}` : ""}
              </Typography>
            </div>
          </li>
        );
      }}
    />
  );
};
