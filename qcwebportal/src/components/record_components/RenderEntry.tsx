import React from "react";
import { Box, Grid } from "@mui/material";
import { Molecule } from "../../PortalTypes";
import { MultiMoleculeViewer, SingleMoleculeViewer } from "../Molecule.tsx";
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

  // Make a list of lists (name, molecule object)
  // Here name is just the index
  const molList: [string, Molecule][] = molecules.map((m, i) => [i.toString(), m]);

  return (
    <Box>
      {molList.length === 1 ? (
        <SingleMoleculeViewer height={250} molecule={molList[0][1]} />
      ) : (
        <MultiMoleculeViewer height={250} molecules={molList} />
      )}
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
