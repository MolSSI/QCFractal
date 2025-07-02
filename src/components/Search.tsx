import * as React from "react";
import { useNavigate, useLocation } from "react-router-dom";
import FormControl from "@mui/material/FormControl";
import InputAdornment from "@mui/material/InputAdornment";
import OutlinedInput from "@mui/material/OutlinedInput";
import SearchRoundedIcon from "@mui/icons-material/SearchRounded";

export default function Search() {
  const navigate = useNavigate();
  const location = useLocation();
  const [input, setInput] = React.useState("");
  
  // Clear input when location changes
  React.useEffect(() => {
    setInput("");
  }, [location.pathname]);
  
  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter" && input.trim()) {
      // Only allow numbers for record id
      const recordId = input.trim();
      if (/^\d+$/.test(recordId)) {
        navigate(`/records/${recordId}`);
      }
    }
  };

  return (
    <FormControl sx={{ width: { xs: "100%", md: "25ch" } }} variant="outlined">
      <OutlinedInput
        size="small"
        id="search"
        placeholder="Lookup Record ID…"
        sx={{ flexGrow: 1 }}
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={handleKeyDown}
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
  );
}
