import * as React from "react";
import { usePageTitle } from "../UsePageTitle.ts";
import {
  Box,
  Stack,
  Typography,
  Button,
  TextField,
  Checkbox,
  Switch,
  FormControlLabel,
  Radio,
  RadioGroup,
  Slider,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  Chip,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  Divider,
  useTheme,
  Paper,
} from "@mui/material";

function ColorSwatch({ label, color }: { label: string; color: string }) {
  return (
    <Stack spacing={1} alignItems="center">
      <Box
        sx={{
          width: 64,
          height: 64,
          bgcolor: color,
          borderRadius: 1,
          border: "1px solid",
          borderColor: "divider",
        }}
      />
      <Typography variant="caption">{label}</Typography>
      <Typography variant="caption" sx={{ fontFamily: "monospace" }}>
        {color}
      </Typography>
    </Stack>
  );
}

export default function ThemePlaygroundPage() {
  usePageTitle("Theme Playground");
  const theme = useTheme();
  const [dialogOpen, setDialogOpen] = React.useState(false);
  const [disabled, setDisabled] = React.useState(false);

  return (
    <Box sx={{ p: 4 }}>
      <Stack spacing={4}>
        <Typography variant="h3">MUI Playground</Typography>

        {/* Global Controls */}
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6">Global Controls</Typography>
          <FormControlLabel
            control={
              <Switch
                checked={disabled}
                onChange={(e) => setDisabled(e.target.checked)}
              />
            }
            label="Disable interactive components"
          />
        </Paper>

        <Divider />

        {/* Colors */}
        <section>
          <Typography variant="h5">Theme Colors</Typography>
          <Stack direction="row" spacing={3} flexWrap="wrap">
            <ColorSwatch label="Primary" color={theme.palette.primary.main} />
            <ColorSwatch
              label="Secondary"
              color={theme.palette.secondary.main}
            />
            <ColorSwatch label="Error" color={theme.palette.error.main} />
            <ColorSwatch label="Warning" color={theme.palette.warning.main} />
            <ColorSwatch label="Info" color={theme.palette.info.main} />
            <ColorSwatch label="Success" color={theme.palette.success.main} />
            <ColorSwatch
              label="Background"
              color={theme.palette.background.default}
            />
            <ColorSwatch label="Paper" color={theme.palette.background.paper} />
          </Stack>
        </section>

        <Divider />

        {/* Typography */}
        <section>
          <Typography variant="h5">Typography</Typography>
          <Stack spacing={1}>
            <Typography variant="h4">Heading 4</Typography>
            <Typography variant="h6">Heading 6</Typography>
            <Typography variant="body1">Body 1 text</Typography>
            <Typography variant="body2">Body 2 text</Typography>
            <Typography variant="caption">Caption text</Typography>
          </Stack>
        </section>

        <Divider />

        {/* Buttons */}
        <section>
          <Typography variant="h5">Buttons</Typography>

          <Stack direction="row" spacing={2}>
            <Button variant="contained" disabled={disabled}>
              Contained
            </Button>
            <Button variant="outlined" disabled={disabled}>
              Outlined
            </Button>
            <Button variant="text" disabled={disabled}>
              Text
            </Button>
          </Stack>

          <Stack direction="row" spacing={2} sx={{ mt: 2 }}>
            <Button variant="contained" color="primary" disabled={disabled}>
              Primary
            </Button>
            <Button variant="contained" color="secondary" disabled={disabled}>
              Secondary
            </Button>
            <Button variant="contained" color="error" disabled={disabled}>
              Error
            </Button>
          </Stack>

          <Stack direction="row" spacing={2} sx={{ mt: 2 }}>
            <Button variant="contained" disabled>
              Disabled
            </Button>
            <Button variant="contained" size="small" disabled={disabled}>
              Small
            </Button>
            <Button variant="contained" size="large" disabled={disabled}>
              Large
            </Button>
          </Stack>
        </section>

        <Divider />

        {/* Inputs */}
        <section>
          <Typography variant="h5">Inputs</Typography>
          <Stack spacing={2}>
            <TextField
              label="Outlined"
              variant="outlined"
              disabled={disabled}
            />
            <TextField
              label="With error"
              error
              helperText="Something went wrong"
              disabled={disabled}
            />

            <FormControl disabled={disabled}>
              <InputLabel>Age</InputLabel>
              <Select defaultValue={10} label="Age">
                <MenuItem value={10}>Ten</MenuItem>
                <MenuItem value={20}>Twenty</MenuItem>
              </Select>
            </FormControl>

            <Slider defaultValue={30} disabled={disabled} />

            <FormControlLabel
              control={<Checkbox disabled={disabled} />}
              label="Checkbox"
            />
            <FormControlLabel
              control={<Switch disabled={disabled} />}
              label="Switch"
            />

            <RadioGroup row defaultValue="a">
              <FormControlLabel
                value="a"
                control={<Radio disabled={disabled} />}
                label="A"
              />
              <FormControlLabel
                value="b"
                control={<Radio disabled={disabled} />}
                label="B"
              />
            </RadioGroup>
          </Stack>
        </section>

        <Divider />

        {/* Feedback */}
        <section>
          <Typography variant="h5">Feedback</Typography>

          <Stack direction="row" spacing={2}>
            <Chip label="Default" />
            <Chip label="Primary" color="primary" />
            <Chip label="Disabled" disabled />
          </Stack>

          <Stack spacing={2} sx={{ mt: 2 }}>
            <Alert severity="success">Success message</Alert>
            <Alert severity="warning">Warning message</Alert>
            <Alert severity="error">Error message</Alert>
          </Stack>
        </section>

        <Divider />

        {/* Dialog */}
        <section>
          <Typography variant="h5">Dialog</Typography>

          <Button
            variant="contained"
            onClick={() => setDialogOpen(true)}
            disabled={disabled}
          >
            Open Dialog
          </Button>

          <Dialog
            open={dialogOpen}
            onClose={() => setDialogOpen(false)}
            aria-describedby="dialog-description"
          >
            <DialogTitle>Example Dialog</DialogTitle>
            <DialogContent>
              <DialogContentText id="dialog-description">
                This is a sample dialog using DialogContentText for proper
                accessibility. It should reflect your typography and spacing
                settings.
              </DialogContentText>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
              <Button variant="contained">Confirm</Button>
            </DialogActions>
          </Dialog>
        </section>
      </Stack>
    </Box>
  );
}
