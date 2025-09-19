import { useNavigate } from "react-router-dom";
import { MoleculeStageProvider, MoleculeViewer } from "../components/Molecule";
import { useEffect, useState } from "react";
import { usePortalClientRequest } from "../usePortalClient";
import * as qcpTypes from "../PortalTypes";
import { FetchedData } from "../PortalClientContext";
import { Typography, Box, Button, TextField } from "@mui/material";
import AddMoleculeModal from "../components/AddMoleculeModal";

const HomePage = () => {
  const navigate = useNavigate();

  /* TESTING */
  const { fetchData } = usePortalClientRequest();

  const [moleculeFetchedData, setMoleculeFetchedData] = useState<
    FetchedData<qcpTypes.Molecule>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  const moleculeId = 105164121;
  useEffect(() => {
    fetchData<qcpTypes.Molecule>(
      setMoleculeFetchedData,
      "get",
      `api/v1/molecules/${moleculeId}`,
    );
  }, [fetchData, moleculeId]);

  const [modalOpen, setModalOpen] = useState(false);
  // currentMolecule may hold a numeric id or an error string to display
  const [currentMoleculeId, setCurrentMoleculeId] = useState<number | string | null>(null);

  const handleAddMolecule = () => setModalOpen(true);
  const handleCloseModal = () => setModalOpen(false);
  const handleSubmitMolecule = (idOrError: number | string) => {
    setCurrentMoleculeId(idOrError);
    // If we have a numeric id, fetch the molecule to display
    if (typeof idOrError === "number") {
      fetchData<qcpTypes.Molecule>(setMoleculeFetchedData, "get", `api/v1/molecules/${idOrError}`);
    }
  };

  return (
    <>
      <Typography variant="h2">Just a sandbox</Typography>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
            <Button variant="outlined" onClick={handleAddMolecule}>
              Add a molecule
            </Button>
          <TextField label="Molecule ID" value={currentMoleculeId ? String(currentMoleculeId) : ""} size="medium" slotProps={{ input: { readOnly: true } }} />
      </Box>

      <button onClick={() => navigate("/projects/11/addRecord")}>Add a record</button>
      <button onClick={() => navigate("/projects/11")}>Go to Project 11</button>
      <button
        onClick={() =>
          navigate(
            "/managers/LilacQM-lx03-f1598182-8306-4d1e-ae03-bc2693e22ed8",
          )
        }
      >
        Go to example manager
      </button>
      <button onClick={() => navigate("/projects/11/records/120382130")}>Example record</button>
      <MoleculeStageProvider width={500} height={500}>
        {moleculeFetchedData.data && (
          <MoleculeViewer moleculeData={moleculeFetchedData?.data} />
        )}
      </MoleculeStageProvider>

      <AddMoleculeModal open={modalOpen} onClose={handleCloseModal} onSubmit={handleSubmitMolecule} />
    </>
  );
};

export default HomePage;
