import { useNavigate } from "react-router-dom";
import { MoleculeStageProvider, MoleculeViewer } from "../components/Molecule";
import { useEffect, useState } from "react";
import { usePortalClientRequest } from "../usePortalClient";
import * as qcpTypes from "../PortalTypes";
import { FetchedData } from "../PortalClientContext";
import { ManagerFragment } from "../components/ManagerFragment.tsx";
import { WaitingReasonFragment } from "../components/WaitingReasonFragment.tsx";
import { Dialog, DialogContent } from "@mui/material";

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
  /* END TESTING */

  const [open, setOpen] = useState(false);
  const [open2, setOpen2] = useState(false);

  return (
    <>
      <h1>Profile</h1>
      <p>This is your profile page.</p>
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
      <MoleculeStageProvider width={500} height={500}>
        {moleculeFetchedData.data && (
          <MoleculeViewer moleculeData={moleculeFetchedData?.data} />
        )}
      </MoleculeStageProvider>

      <button
        onClick={() => {
          setOpen(true);
        }}
      >
        Open Dialog
      </button>
      <Dialog
        open={open}
        onClose={() => {
          setOpen(false);
        }}
      >
        {open && (
          <>
            <DialogContent>
              <ManagerFragment managerName="LilacQM-lx03-f1598182-8306-4d1e-ae03-bc2693e22ed8" />
            </DialogContent>
          </>
        )}
      </Dialog>

      <button
        onClick={() => {
          setOpen2(true);
        }}
      >
        Open Dialog
      </button>
      <Dialog
        fullWidth={true}
        open={open2}
        onClose={() => {
          setOpen2(false);
        }}
      >
        {open2 && (
          <>
            <DialogContent>
              <WaitingReasonFragment recordId={123960520} />
            </DialogContent>
          </>
        )}
      </Dialog>
    </>
  );
};

export default HomePage;
