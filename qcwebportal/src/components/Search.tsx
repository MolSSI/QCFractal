import * as React from "react";
import { useNavigate } from "react-router-dom";
import {
  Autocomplete,
  Box,
  CircularProgress,
  FormControl,
  InputAdornment,
  TextField,
  Typography,
} from "@mui/material";
import SearchRoundedIcon from "@mui/icons-material/SearchRounded";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import { useQuery } from "@tanstack/react-query";
import RecordTypeChip from "./RecordTypeChip.tsx";
import { matchesTokens } from "../Utils.ts";

type SearchOption =
  | { type: "project"; data: qcpTypes.ProjectListEntry }
  | { type: "dataset"; data: qcpTypes.DatasetListEntry }
  | { type: "record"; id: string };

export default function Search() {
  const navigate = useNavigate();
  const { makeRequest } = usePortalClient();
  const [open, setOpen] = React.useState(false);
  const [inputValue, setInputValue] = React.useState("");

  const { data: projects = [], isLoading: projectsLoading } = useQuery({
    queryKey: ["listProjects"],
    queryFn: () =>
      makeRequest<qcpTypes.ProjectListEntry[]>("GET", "/api/v1/projects"),
    enabled: open,
  });

  const { data: datasets = [], isLoading: datasetsLoading } = useQuery({
    queryKey: ["listDatasets"],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetListEntry[]>("GET", "/api/v1/datasets"),
    enabled: open,
  });

  const options = React.useMemo(() => {
    const opts: SearchOption[] = [];
    const trimmedInput = inputValue.trim();

    // Filter projects
    projects.forEach((p) => {
      if (
        matchesTokens(trimmedInput, p.project_name) ||
        matchesTokens(trimmedInput, p.tagline) ||
        p.id.toString().includes(trimmedInput)
      ) {
        opts.push({ type: "project", data: p });
      }
    });

    // Filter datasets
    datasets.forEach((d) => {
      if (
        matchesTokens(trimmedInput, d.dataset_name) ||
        matchesTokens(trimmedInput, d.tagline) ||
        d.id.toString().includes(trimmedInput)
      ) {
        opts.push({ type: "dataset", data: d });
      }
    });

    // Add record ID option if numeric
    if (/^\d+$/.test(trimmedInput)) {
      opts.push({ type: "record", id: trimmedInput });
    }

    return opts;
  }, [projects, datasets, inputValue]);

  const isLoading = projectsLoading || datasetsLoading;

  return (
    <FormControl sx={{ width: { xs: "100%", md: "60ch" } }} variant="outlined">
      <Autocomplete
        size="small"
        filterOptions={(x) => x}
        open={open && inputValue.trim().length > 0}
        onOpen={() => setOpen(true)}
        onClose={() => setOpen(false)}
        inputValue={inputValue}
        onInputChange={(_event, newInputValue) => {
          setInputValue(newInputValue);
        }}
        onChange={(_event, newValue) => {
          if (newValue) {
            if (newValue.type === "project") {
              navigate(`/projects/${newValue.data.id}`);
            } else if (newValue.type === "dataset") {
              navigate(`/datasets/${newValue.data.id}`);
            } else if (newValue.type === "record") {
              navigate(`/records/${newValue.id}`);
            }
            setInputValue("");
          }
        }}
        options={options}
        loading={isLoading}
        groupBy={(option) => {
          if (option.type === "project") return "Projects";
          if (option.type === "dataset") return "Datasets";
          return "Records";
        }}
        getOptionLabel={(option) => {
          if (option.type === "project") return option.data.project_name;
          if (option.type === "dataset") return option.data.dataset_name;
          return option.id;
        }}
        isOptionEqualToValue={(option, value) => {
          if (option.type !== value.type) return false;
          if (option.type === "project" && value.type === "project")
            return option.data.id === value.data.id;
          if (option.type === "dataset" && value.type === "dataset")
            return option.data.id === value.data.id;
          if (option.type === "record" && value.type === "record")
            return option.id === value.id;
          return false;
        }}
        renderInput={(params) => (
          <TextField
            {...params}
            size="small"
            placeholder="Search Projects, Datasets, Records…"
            InputProps={{
              ...params.InputProps,
              startAdornment: (
                <InputAdornment position="start" sx={{ color: "text.primary" }}>
                  <SearchRoundedIcon fontSize="small" />
                </InputAdornment>
              ),
              endAdornment: (
                <React.Fragment>
                  {isLoading ? (
                    <CircularProgress color="inherit" size={16} />
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
              <Box>
                {option.type === "project" && (
                  <>
                    <Typography variant="body1">
                      {option.data.id}: {option.data.project_name}
                    </Typography>
                    <Typography variant="caption" color="textSecondary">
                      {option.data.tagline}
                    </Typography>
                  </>
                )}
                {option.type === "dataset" && (
                  <>
                    <Typography variant="body1">
                      {option.data.id}: {option.data.dataset_name}
                    </Typography>
                    <Typography variant="caption" color="textSecondary">
                      <RecordTypeChip type={option.data.dataset_type} />{" "}
                      {option.data.tagline ? `- ${option.data.tagline}` : ""}
                    </Typography>
                  </>
                )}
                {option.type === "record" && (
                  <Typography variant="body1">
                    Go to record ID: {option.id}
                  </Typography>
                )}
              </Box>
            </li>
          );
        }}
      />
    </FormControl>
  );
}
