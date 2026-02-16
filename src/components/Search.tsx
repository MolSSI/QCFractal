import * as React from "react";
import { useNavigate } from "react-router-dom";
import FormControl from "@mui/material/FormControl";
import InputAdornment from "@mui/material/InputAdornment";
import OutlinedInput from "@mui/material/OutlinedInput";
import SearchRoundedIcon from "@mui/icons-material/SearchRounded";
import Snackbar from "@mui/material/Snackbar";
import Alert from "@mui/material/Alert";

export const server_address = import.meta.env.VITE_QCFRACTAL_URI;

export default function Search() {
  const navigate = useNavigate();
  const [input, setInput] = React.useState("");
  const [errorOpen, setErrorOpen] = React.useState(false);
  const [errorMsg, setErrorMsg] = React.useState("");

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const recordId = input.trim();

    if (!/^\d+$/.test(recordId)) {
      setErrorMsg("Please enter a valid numeric record ID.");
      setErrorOpen(true);
      return;
    }

    try {
      const res = await fetch(`${server_address}/api/v1/records/${recordId}`);
      if (res.ok) {
        navigate(`/records/${recordId}`);
      } else if (res.status === 404) {
        setErrorMsg("No record found with that ID.");
        setErrorOpen(true);
      } else {
        setErrorMsg("Non-existing record ID");
        setErrorOpen(true);
      }
    } catch {
      setErrorMsg("Network error. Please try again.");
      setErrorOpen(true);
    }
  };

  return (
    <>
      <form onSubmit={handleSubmit}>
        <FormControl
          sx={{ width: { xs: "100%", md: "25ch" } }}
          variant="outlined"
        >
          <OutlinedInput
            size="small"
            id="search"
            placeholder="Lookup Record ID…"
            sx={{ flexGrow: 1 }}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            startAdornment={
              <InputAdornment position="start" sx={{ color: "text.primary" }}>
                <SearchRoundedIcon fontSize="small" />
              </InputAdornment>
            }
            inputProps={{
              "aria-label": "lookup record id",
            }}
          />
        </FormControl>
      </form>
      <Snackbar
        open={errorOpen}
        autoHideDuration={4000}
        onClose={() => setErrorOpen(false)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        <Alert
          severity="error"
          onClose={() => setErrorOpen(false)}
          sx={{ width: "100%" }}
        >
          {errorMsg}
        </Alert>
      </Snackbar>
    </>
  );
}
