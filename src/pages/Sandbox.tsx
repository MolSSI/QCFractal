import { useNavigate } from "react-router-dom";
import { usePageTitle } from "../UsePageTitle.ts";
import { MoleculeStageProvider, MoleculeViewer } from "../components/Molecule";
import { useState } from "react";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import { Box, Button, Typography } from "@mui/material";
import AddMoleculeModal from "../components/AddMoleculeModal";
import { useQuery } from "@tanstack/react-query";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";

const SandboxPage = () => {
  usePageTitle("Sandbox");
  const navigate = useNavigate();

  const { makeRequest } = usePortalClient();

  const moleculeId = 105164121;

  const [modalOpen, setModalOpen] = useState(false);
  // currentMolecule may hold a numeric id or an error string to display
  const [currentMoleculeId, setCurrentMoleculeId] = useState<
    number | string | null
  >(moleculeId);

  const moleculeQueryId =
    typeof currentMoleculeId === "number" ? currentMoleculeId : null;

  const {
    status: moleculeStatus,
    data: moleculeData,
    error: moleculeError,
  } = useQuery({
    queryKey: ["molecule", moleculeQueryId],
    queryFn: () =>
      makeRequest<qcpTypes.Molecule>(
        "GET",
        `api/v1/molecules/${moleculeQueryId}`,
      ),
    enabled: moleculeQueryId !== null,
  });

  const handleAddMolecule = () => setModalOpen(true);
  const handleCloseModal = () => setModalOpen(false);
  const handleSubmitMolecule = (idOrError: number | string) => {
    setCurrentMoleculeId(idOrError);
  };

  return (
    <>
      <Typography variant="h2">Just a sandbox</Typography>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
        <Button variant="outlined" onClick={handleAddMolecule}>
          Add a molecule
        </Button>
        <Box sx={{ display: "flex", flexDirection: "column" }}>
          <Typography variant="caption" color="text.secondary">
            Molecule ID
          </Typography>
          <Typography variant="body1" sx={{ mt: 0.5 }}>
            {currentMoleculeId ?? ""}
          </Typography>
        </Box>
      </Box>

      <button onClick={() => navigate("/projects/11/addRecord")}>
        Add a record
      </button>
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
      <button onClick={() => navigate("/projects/11/records/120382130")}>
        Example record
      </button>
      <Box sx={{ width: 500, height: 500, position: "relative" }}>
        {moleculeQueryId === null ? (
          <LoadingIndicator message="No molecule selected." />
        ) : moleculeStatus === "pending" ? (
          <LoadingIndicator message="Loading molecule..." />
        ) : moleculeStatus === "error" ? (
          <ErrorIndicator
            message={moleculeError?.message ?? "Failed to load molecule."}
          />
        ) : (
          <MoleculeStageProvider width={500} height={500}>
            {moleculeData && <MoleculeViewer moleculeData={moleculeData} />}
          </MoleculeStageProvider>
        )}
      </Box>

      <AddMoleculeModal
        open={modalOpen}
        onClose={handleCloseModal}
        onSubmit={handleSubmitMolecule}
      />
    </>
  );
};

export default SandboxPage;
