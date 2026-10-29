import { useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Button,
  Divider,
  Grid,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import DownloadIcon from "@mui/icons-material/Download";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { usePageTitle } from "../UsePageTitle.ts";
import { isNotFoundError, retryUnlessNotFound } from "../Utils.ts";
import {
  MolecularFormula,
  SingleMoleculeViewer,
} from "../components/Molecule.tsx";
import { GenericDataList } from "../components/GenericDataList.tsx";
import JsonViewer from "../components/JsonViewer.tsx";
import MoleculeRecordTable from "../components/MoleculeRecordTable.tsx";

function downloadMolecule(molecule: qcpTypes.Molecule) {
  const blob = new Blob([JSON.stringify(molecule, null, 2)], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `molecule_${molecule.id}.json`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function MoleculeGeometryTable({ molecule }: { molecule: qcpTypes.Molecule }) {
  return (
    <TableContainer sx={{ maxHeight: 400 }}>
      <Table size="small" stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>#</TableCell>
            <TableCell>Symbol</TableCell>
            <TableCell align="right">x (bohr)</TableCell>
            <TableCell align="right">y (bohr)</TableCell>
            <TableCell align="right">z (bohr)</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {molecule.symbols.map((symbol, i) => (
            <TableRow key={i}>
              <TableCell>{i}</TableCell>
              <TableCell>{symbol}</TableCell>
              {[0, 1, 2].map((axis) => (
                <TableCell
                  key={axis}
                  align="right"
                  sx={{ fontFamily: "monospace" }}
                >
                  {molecule.geometry[3 * i + axis]?.toFixed(8)}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}

function MoleculeDetails({ molecule }: { molecule: qcpTypes.Molecule }) {
  const otherIdentifiers = Object.fromEntries(
    Object.entries(molecule.identifiers ?? {}).filter(
      ([key]) => key !== "molecule_hash" && key !== "molecular_formula",
    ),
  );

  const details = {
    molecular_charge: molecule.molecular_charge,
    molecular_multiplicity: molecule.molecular_multiplicity,
    atom_count: molecule.symbols.length,
    fragment_count: molecule.fragments?.length,
    fragment_charges: molecule.fragment_charges,
    fragment_multiplicities: molecule.fragment_multiplicities,
    molecule_hash: molecule.identifiers?.molecule_hash,
    identifiers: otherIdentifiers,
    comment: molecule.comment,
    fix_com: molecule.fix_com,
    fix_orientation: molecule.fix_orientation,
    validated: molecule.validated,
  };

  return (
    <>
      <GenericDataList
        data={details}
        keys={[
          "molecular_charge",
          "molecular_multiplicity",
          "atom_count",
          "fragment_count",
          "fragment_charges",
          "fragment_multiplicities",
          { key: "molecule_hash", label: "Molecule hash" },
          "identifiers",
          { key: "comment", showIfEmpty: false },
          { key: "fix_com", label: "Fix COM" },
          "fix_orientation",
          "validated",
        ]}
      />
      {molecule.extras && Object.keys(molecule.extras).length > 0 && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="subtitle2">Extras</Typography>
          <JsonViewer value={molecule.extras} sortKeys />
        </Box>
      )}
    </>
  );
}

function Molecule() {
  const { moleculeId } = useParams();
  const { makeRequest } = usePortalClient();

  const parsedMoleculeId = moleculeId ? Number(moleculeId) : NaN;
  const validMoleculeId = Number.isFinite(parsedMoleculeId);

  const {
    status: moleculeStatus,
    data: molecule,
    error: moleculeError,
  } = useQuery({
    queryKey: ["molecule", parsedMoleculeId],
    queryFn: () =>
      makeRequest<qcpTypes.Molecule>(
        "GET",
        `/api/v1/molecules/${parsedMoleculeId}`,
      ),
    enabled: validMoleculeId,
    retry: retryUnlessNotFound,
  });

  const pageTitle = molecule?.name
    ? `Molecule ${parsedMoleculeId}: ${molecule.name}`
    : `Molecule ${parsedMoleculeId}`;
  usePageTitle(pageTitle);

  if (!validMoleculeId) {
    return <ErrorIndicator fullPage message="Invalid molecule ID" />;
  }
  if (moleculeStatus === "pending") {
    return <LoadingIndicator fullPage />;
  }
  if (moleculeStatus === "error") {
    const message = isNotFoundError(moleculeError)
      ? `Molecule ${parsedMoleculeId} does not exist`
      : moleculeError.message;
    return <ErrorIndicator fullPage message={message} />;
  }
  if (!molecule) {
    return <ErrorIndicator fullPage message="Molecule not found" />;
  }

  return (
    <>
      <Stack spacing={0.5} sx={{ width: "100%" }}>
        <Stack direction="row" alignItems="center" flexWrap="wrap" gap={2}>
          <Typography variant="h5" fontWeight="bold">
            {molecule.name ? molecule.name.trim() : `Molecule ${molecule.id}`}
          </Typography>
          <Button
            variant="outlined"
            size="small"
            startIcon={<DownloadIcon />}
            onClick={() => downloadMolecule(molecule)}
          >
            Download
          </Button>
        </Stack>
        <Typography variant="body1">
          <MolecularFormula molecule={molecule} />
        </Typography>
        <Typography variant="body2" color="text.secondary">
          ID {molecule.id}
        </Typography>
      </Stack>
      <Divider sx={{ my: 2, width: "100%" }} />

      <Grid container spacing={2} sx={{ width: "100%" }}>
        <Grid size={{ xs: 12, md: 6 }}>
          <MoleculeDetails molecule={molecule} />
          {/* Own wrapper so the theme's :last-of-type rule keeps its bottom border */}
          <Box sx={{ mt: 2 }}>
            <Accordion disableGutters>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography variant="subtitle1" fontWeight="bold">
                  Geometry ({molecule.symbols.length} atoms)
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                <MoleculeGeometryTable molecule={molecule} />
              </AccordionDetails>
            </Accordion>
          </Box>
        </Grid>
        <Grid size={{ xs: 12, md: 6 }}>
          <SingleMoleculeViewer
            molecule={molecule}
            height={400}
            linkId={false}
          />
        </Grid>
      </Grid>

      <Divider sx={{ my: 2, width: "100%" }} />
      <MoleculeRecordTable key={molecule.id} moleculeId={molecule.id} />
    </>
  );
}

export default Molecule;
