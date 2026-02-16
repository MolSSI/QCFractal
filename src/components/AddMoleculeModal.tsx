import React, { useState } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
  Box,
  TextField,
  Button,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
} from "@mui/material";
import { server_address } from "../request_config";

type Props = {
  open: boolean;
  onClose: () => void;
  // onSubmit receives either a numeric id (success) or an error string to display
  onSubmit: (moleculeIdOrError: number | string) => void;
};

const AddMoleculeModal: React.FC<Props> = ({ open, onClose, onSubmit }) => {
  const [tabIndex, setTabIndex] = useState<number>(0);
  const [pasteText, setPasteText] = useState<string>("");
  const [uploadedName, setUploadedName] = useState<string | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [pastedFilename, setPastedFilename] =
    useState<string>("pastedMolecule");
  const [pastedFormat, setPastedFormat] = useState<string>("xyz");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  // compact reusable styling for the multiline text area
  const textFieldSx = {
    bgcolor: "transparent",
    borderRadius: 1,
    width: "100%",
    height: 200,
    "& .MuiOutlinedInput-root": {
      height: "100%",
      display: "flex",
      flexDirection: "column",
      alignItems: "stretch",
      paddingTop: 1,
      paddingBottom: 1,
      background: "transparent",
      boxShadow: "none",
    },
    // target the multiline textarea class so styles apply when `multiline` is used
    "& .MuiInputBase-inputMultiline": {
      boxSizing: "border-box",
      whiteSpace: "pre",
      flex: 1,
      color: "#fff",
      outline: "none",
      maxHeight: "100%",
      overflow: "auto",
    },
  } as const;

  const handleTabChange = (_: React.SyntheticEvent, newValue: number) => {
    setTabIndex(newValue);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files && e.target.files[0];
    setSelectedFile(f ?? null);
    setUploadedName(f ? f.name : null);
  };

  const handleSubmit = async () => {
    setErrorMessage(null);
    setLoading(true);

    try {
      const form = new FormData();

      if (tabIndex === 0) {
        // Use filename and selected format from inputs
        const pasteFilename = `${pastedFilename}.${pastedFormat}`;
        const mime =
          pastedFormat === "json" ? "application/json" : "chemical/x-xyz";
        const blob = new Blob([pasteText || ""], { type: mime });
        form.append("files", blob, pasteFilename);
        // include filename field for backends that expect it
        form.append("filename", pasteFilename);
      } else if (tabIndex === 1) {
        if (!selectedFile) {
          setErrorMessage("No file selected");
          setLoading(false);
          return;
        }
        form.append("files", selectedFile, selectedFile.name);
      } else {
        // draw tab - not implemented
        setErrorMessage("Draw option not implemented");
        setLoading(false);
        return;
      }

      const bodyDataObj = {};
      const bodyDataBlob = new Blob([JSON.stringify(bodyDataObj)], {
        type: "application/json",
      });
      // append body_data first (order doesn't strictly matter)
      form.append("body_data", bodyDataBlob, "body_data");

      const resp = await fetch(`${server_address}/api/v1/molecules/fromFiles`, {
        method: "POST",
        body: form,
        credentials: "include",
      });

      if (!resp.ok) {
        const text = await resp.text();
        setErrorMessage(
          `Upload failed: ${resp.status} ${resp.statusText} ${text}`,
        );
        setLoading(false);
        return;
      }

      const json = await resp.json().catch(() => null);
      // Log full server response for debugging/inspection
      console.debug("molecules/fromFiles response:", json);

      // server may return several shapes. handle the shape: [{ filename: [[name, id], ...] }, ...]
      let extractedId: number | null = null;
      try {
        if (
          Array.isArray(json) &&
          json.length > 0 &&
          typeof json[0] === "object"
        ) {
          const first = json[0] as Record<string, unknown>;
          const keys = Object.keys(first);
          if (keys.length > 0) {
            const arr = first[keys[0]] as unknown;
            if (Array.isArray(arr) && arr.length > 0 && Array.isArray(arr[0])) {
              const firstTuple = arr[0] as unknown[];
              // tuple expected [name, id]
              if (firstTuple.length >= 2 && typeof firstTuple[1] === "number") {
                extractedId = firstTuple[1] as number;
              }
            }
          }
        }
      } catch (e) {
        console.debug("id extraction failed", e);
      }

      // fallback to previous patterns
      if (extractedId === null) {
        const extractId = (obj: any): number | null => {
          if (!obj) return null;
          if (typeof obj.id === "number") return obj.id;
          if (obj.data && typeof obj.data.id === "number") return obj.data.id;
          if (typeof obj.molecule_id === "number") return obj.molecule_id;
          if (obj.molecule && typeof obj.molecule.id === "number")
            return obj.molecule.id;
          return null;
        };
        const id = extractId(json);
        if (typeof id === "number") extractedId = id;
      }

      if (typeof extractedId === "number") {
        onSubmit(extractedId);
      } else {
        // no id: surface a helpful error message to the Home text field
        let errMsg = "No molecule id returned from server";
        // if server returned a message field, use it
        try {
          if (json && typeof json === "object") {
            const j = json as Record<string, unknown>;
            if (typeof j["msg"] === "string") errMsg = j["msg"] as string;
          }
        } catch {
          /* ignore */
        }
        onSubmit(errMsg);
      }
      // reset
      setPasteText("");
      setUploadedName(null);
      setSelectedFile(null);
      onClose();
    } catch (err: unknown) {
      // safer handling for unknown error type
      const msg = err instanceof Error ? err.message : String(err);
      setErrorMessage(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>Add a molecule</DialogTitle>
      <DialogContent>
        <Box sx={{ borderBottom: "1px solid rgba(255,255,255,0.12)", pb: 1 }}>
          <Tabs
            value={tabIndex}
            onChange={handleTabChange}
            aria-label="Add molecule tabs"
            sx={{ minHeight: 48 }}
          >
            <Tab label="Copy and paste" />
            <Tab label="Upload" />
            <Tab label="Draw" />
          </Tabs>
        </Box>

        <Box sx={{ mt: 2, minHeight: 260, p: 0 }}>
          {/* Dark content pane */}
          <Box
            sx={{
              border: "none",
              bgcolor: "#0d1114",
              p: 2,
              display: "flex",
              flexDirection: "column",
            }}
          >
            {tabIndex === 0 && (
              <>
                <TextField
                  fullWidth
                  multiline
                  rows={8}
                  value={pasteText}
                  onChange={(e) => setPasteText(e.target.value)}
                  variant="outlined"
                  sx={{ ...textFieldSx }}
                />

                <Box
                  sx={{ display: "flex", gap: 2, alignItems: "center", mt: 1 }}
                >
                  <TextField
                    label="Filename"
                    value={pastedFilename}
                    onChange={(e) => setPastedFilename(e.target.value)}
                    size="small"
                    sx={{ bgcolor: "transparent", input: { color: "#fff" } }}
                  />

                  <FormControl size="small" sx={{ minWidth: 120 }}>
                    <InputLabel sx={{ color: "#fff" }}>Format</InputLabel>
                    <Select
                      value={pastedFormat}
                      label="Format"
                      onChange={(e) => setPastedFormat(e.target.value)}
                      sx={{ color: "#fff" }}
                    >
                      <MenuItem value="xyz">xyz</MenuItem>
                      <MenuItem value="json">json</MenuItem>
                    </Select>
                  </FormControl>
                </Box>
              </>
            )}

            {tabIndex === 1 && (
              <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
                <Button variant="contained" component="label">
                  Choose file
                  <input hidden type="file" onChange={handleFileChange} />
                </Button>
                <Typography variant="body2" sx={{ color: "#fff" }}>
                  {uploadedName ?? "No file chosen"}
                </Typography>
              </Box>
            )}

            {tabIndex === 2 && (
              <Box>
                <Typography variant="body2" color="text.secondary">
                  Draw option coming soon.
                </Typography>
              </Box>
            )}
          </Box>

          {/* helper prompt moved below the dark pane */}
          {tabIndex === 0 && (
            <>
              <Typography
                variant="caption"
                sx={{ color: "#fff", mt: 1, display: "block" }}
              >
                Please paste the molecule data here
              </Typography>
            </>
          )}

          {errorMessage && (
            <Typography variant="body2" sx={{ color: "error.main", mt: 1 }}>
              {errorMessage}
            </Typography>
          )}
        </Box>
      </DialogContent>

      <DialogActions>
        <Box sx={{ flex: 1 }} />
        <Button onClick={onClose}>Cancel</Button>
        <Button
          onClick={handleSubmit}
          disabled={
            loading ||
            (tabIndex === 0
              ? pasteText.trim() === ""
              : tabIndex === 1
                ? !selectedFile
                : true)
          }
          sx={{
            minWidth: 96,
            color: "#fff",
            backgroundColor: "transparent",
            border: "1px solid rgba(255,255,255,0.28)",
            boxShadow: "none",
            "&:hover": {
              backgroundColor: "rgba(255,255,255,0.04)",
            },
            "&.Mui-disabled": {
              color: "rgba(255,255,255,0.6)",
              borderColor: "rgba(255,255,255,0.12)",
              backgroundColor: "transparent",
              boxShadow: "none",
            },
          }}
        >
          {loading ? "Uploading…" : "Submit"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default AddMoleculeModal;
