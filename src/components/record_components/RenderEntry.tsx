import React from "react";
import { Box, Grid, Typography } from "@mui/material";
import { Molecule } from "../../PortalTypes";
import { MoleculeStageProvider, MoleculeViewer } from "../Molecule.tsx";
import { GenericDataList, GenericDataListKey } from "../GenericDataList.tsx";

export interface RenderEntryProps {
  molecules: Molecule[];
  data: Record<string, unknown>;
  keys: GenericDataListKey[];
}

export interface EntryMoleculesProps {
  molecules: Molecule[];
}

export const EntryMolecules: React.FC<EntryMoleculesProps> = ({
  molecules,
}) => {
  if (molecules.length === 0) {
    return null;
  }

  const mol = molecules[0];

  return (
    <Box>
      <Typography variant="subtitle2" gutterBottom>
        Molecule
      </Typography>
      <Box
        sx={{
          width: "100%",
          height: 200,
          backgroundColor: "#f5f5f5",
          borderRadius: 1,
          overflow: "hidden",
        }}
      >
        <MoleculeStageProvider width="100%" height={200}>
          <MoleculeViewer moleculeData={mol} />
        </MoleculeStageProvider>
      </Box>
    </Box>
  );
};



export const RenderEntry: React.FC<RenderEntryProps> = ({
  molecules,
  data,
  keys,
}) => {
  return (
    <Box sx={{ width: "100%" }}>
      <Grid container spacing={4}>
        {/* Left Column: Molecule Data */}
        <Grid size={3}>
          <EntryMolecules molecules={molecules} />
        </Grid>

        {/* Right Column: Other Entry Info */}
        <Grid size={9}>
          <GenericDataList data={data} keys={keys} />
        </Grid>
      </Grid>
    </Box>
  );
};
