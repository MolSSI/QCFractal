import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  ReactNode,
} from "react";
import {
  Box,
  FormControl,
  MenuItem,
  Select,
  Typography,
  useTheme,
  IconButton,
} from "@mui/material";
import {
  NavigateBefore as NavigateBeforeIcon,
  NavigateNext as NavigateNextIcon,
} from "@mui/icons-material";
import { moleculeToSDF } from "../MoleculeUtils";
import * as qcpTypes from "../PortalTypes";
import { Stage, Component } from "ngl";
import ErrorIndicator from "./ErrorIndicator.tsx";

type MolecularFormulaProps = {
  molecule: qcpTypes.Molecule;
};

function MolecularFormula({ molecule }: MolecularFormulaProps) {
  // Possible that molecular formula isn't in identifiers, so compute by hand
  // Plus, we want subscripts and superscripts
  const atomCounts = molecule.symbols.reduce(
    (acc: Record<string, number>, symbol: string) => {
      const capSym = symbol[0].toUpperCase() + symbol.substring(1)?.toLowerCase();
      acc[capSym] = (acc[capSym] || 0) + 1;
      return acc;
    },
    {},
  );

  const elements = Object.keys(atomCounts);

  const hasCarbon = elements.includes("C");

  const orderedElements = elements.sort((a, b) => {
    if (hasCarbon) {
      if (a === "C") return -1;
      if (b === "C") return 1;

      if (a === "H") return b === "C" ? 1 : -1;
      if (b === "H") return a === "C" ? -1 : 1;
    }
    return a.localeCompare(b);
  });

  return (
    <>
      {orderedElements.map((symbol: string) => (
        <span key={symbol}>
          {symbol}
          {atomCounts[symbol] > 1 && <sub>{atomCounts[symbol]}</sub>}
        </span>
      ))}
    </>
  );
}

const StageContext = createContext<Stage | undefined>(undefined);

type MoleculeStageProviderProps = {
  width: number | string;
  height: number | string;
  children: ReactNode;
};

function MoleculeStageProvider({
  width,
  height,
  children,
}: MoleculeStageProviderProps) {
  const stageElementRef = useRef<HTMLDivElement>(null);
  const [stage, setStage] = useState<Stage | null>(null);
  const theme = useTheme();

  useEffect(() => {
    const newStage = new Stage(stageElementRef.current!, {
      backgroundColor: theme.palette.background.paper,
    });

    // NGL console.logs "STAGE LOG loading/loaded file" on every molecule it
    // loads. Keep its log list, but keep the console clean
    newStage.log = (msg: string) => {
      newStage.logList.push(msg);
    };

    setStage(newStage);

    const handleResize = () => newStage.handleResize();
    window.addEventListener("resize", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
      newStage.dispose();
    };
  }, [theme.palette.background.paper]);

  return (
    <>
      <div ref={stageElementRef} style={{ width, height }} />
      {stage && (
        <StageContext.Provider value={stage}>
          {children}
        </StageContext.Provider>
      )}
    </>
  );
}

const MoleculeViewer: React.FC<{ moleculeData: qcpTypes.Molecule }> = ({
  moleculeData,
}) => {
  const stage = useContext(StageContext);

  if (!stage) {
    throw new Error(
      "MoleculeViewer must be used within a MoleculeStageProvider",
    );
  }
  const componentRef = useRef<Component>(null);

  useEffect(() => {
    // Remove the previous component if it exists
    if (componentRef.current) {
      stage.removeComponent(componentRef.current);
      componentRef.current = null;
    }

    if (!moleculeData) return;
    if (!stage) return;

    const molStr = moleculeToSDF(moleculeData);
    const molBlob = new Blob([molStr], { type: "text/plain" });

    stage
      .loadFile(molBlob, { ext: "sdf", defaultRepresentation: true })
      .then(function (comp) {
        if (comp instanceof Component) {
          componentRef.current = comp;
        }
        comp!.addRepresentation("ball+stick", { multipleBond: true });
        stage.autoView();
      });
  }, [stage, moleculeData]);

  // Stage is rendered in the provider
  return null;
};

type SingleMoleculeViewerProps = {
  molecule?: qcpTypes.Molecule;
  title?: string;
  width?: number | string;
  height?: number;
};

const SingleMoleculeViewer: React.FC<SingleMoleculeViewerProps> = ({
  molecule,
  title,
  width = "100%",
  height = 400,
}) => {
  const theme = useTheme();

  if (!molecule) {
    return <ErrorIndicator message="Molecule data not available" />;
  }

  return (
    <Box
      sx={{
        width,
        height,
        display: "flex",
        flexDirection: "column",
        alignItems: "stretch",
        border: `1px solid ${theme.palette.divider}`,
        borderRadius: 1,
        overflow: "hidden",
        bgcolor: "background.paper",
      }}
    >
      <Box sx={{ p: 0.5 }}>
        <Typography variant="subtitle2" sx={{ textAlign: "center" }}>
          {title && (
            <>
              {title} (<MolecularFormula molecule={molecule} />)
            </>
          )}
          {!title && (
            <>
              <MolecularFormula molecule={molecule} />
            </>
          )}
        </Typography>
        <Typography variant="caption" sx={{ display: "block", textAlign: "center" }}>
          ID {molecule.id}
        </Typography>
      </Box>
      <Box sx={{ flexGrow: 1, width: "100%", position: "relative" }}>
        <MoleculeStageProvider width="100%" height="100%">
          <MoleculeViewer moleculeData={molecule} />
        </MoleculeStageProvider>
      </Box>
    </Box>
  );
};

type MultiMoleculeViewerProps = {
  molecules: [string, qcpTypes.Molecule][];
  title?: string;
  width?: number | string;
  height?: number;
};

const MultiMoleculeViewer: React.FC<MultiMoleculeViewerProps> = ({
  molecules,
  title,
  width = "100%",
  height = 400,
}) => {
  const [index, setIndex] = useState(0);
  const theme = useTheme();

  if (!molecules || molecules.length === 0) {
    return <Typography>No molecules to display</Typography>;
  }

  const currentMolecule = molecules[index][1];

  const handleNext = () => {
    setIndex((prev) => (prev + 1) % molecules.length);
  };

  const handlePrev = () => {
    setIndex((prev) => (prev - 1 + molecules.length) % molecules.length);
  };

  return (
    <Box
      sx={{
        width,
        height,
        display: "flex",
        flexDirection: "column",
        alignItems: "stretch",
        border: `1px solid ${theme.palette.divider}`,
        borderRadius: 1,
        overflow: "hidden",
        bgcolor: "background.paper",
      }}
    >
      <Box sx={{ p: 0.5 }}>
        <Typography variant="subtitle2" sx={{ textAlign: "center" }}>
          {title && <>{title}</>}
          {!title && (
            <>
              <MolecularFormula molecule={currentMolecule} />
            </>
          )}
        </Typography>
        <Typography variant="caption" sx={{ display: "block", textAlign: "center" }}>
          ID {currentMolecule.id}
        </Typography>
      </Box>
      <Box sx={{ flexGrow: 1, width: "100%", position: "relative" }}>
        <MoleculeStageProvider width="100%" height="100%">
          <MoleculeViewer moleculeData={currentMolecule} />
        </MoleculeStageProvider>
      </Box>

      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          gap: 1,
          p: 0.5,
          borderTop: `1px solid ${theme.palette.divider}`,
          bgcolor: "background.default",
        }}
      >
        <IconButton
          size="small"
          onClick={handlePrev}
          disabled={molecules.length <= 1}
        >
          <NavigateBeforeIcon fontSize="small" />
        </IconButton>

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <Select
            value={index}
            onChange={(e) => setIndex(Number(e.target.value))}
            sx={{
              fontSize: "0.75rem",
              ".MuiSelect-select": {
                py: 0.5,
                px: 1,
              },
            }}
          >
            {molecules.map(([name], i) => (
              <MenuItem key={i} value={i} sx={{ fontSize: "0.75rem" }}>
                {name}
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        <Typography
          variant="caption"
          sx={{ minWidth: 60, textAlign: "center" }}
        >
          {index + 1} / {molecules.length}
        </Typography>

        <IconButton
          size="small"
          onClick={handleNext}
          disabled={molecules.length <= 1}
        >
          <NavigateNextIcon fontSize="small" />
        </IconButton>
      </Box>
    </Box>
  );
};

export default { MoleculeStageProvider, MoleculeViewer, SingleMoleculeViewer, MultiMoleculeViewer, MolecularFormula };
export { MoleculeStageProvider, MoleculeViewer, SingleMoleculeViewer, MultiMoleculeViewer, MolecularFormula };
