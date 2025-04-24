import { useNavigate } from "react-router-dom";
import { MoleculeStageProvider, MoleculeViewer } from "../components/Molecule";
import { useEffect, useState } from "react";
import { usePortalClientRequest } from "../usePortalClient";
import * as qcpTypes from "../PortalTypes";
import { FetchedData } from "../PortalClientContext";

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

  return (
    <>
      <h1>Profile</h1>
      <p>This is your profile page.</p>
      <button onClick={() => navigate("/projects/11")}>Go to Project 11</button>
      <button onClick={() => navigate("/projects/12")}>Go to Project 12</button>
      <button onClick={() => navigate("/projects/11/records/120382130")}>
        Example record
      </button>
      <MoleculeStageProvider width={500} height={500}>
        {moleculeFetchedData.data && (
          <MoleculeViewer moleculeData={moleculeFetchedData?.data} />
        )}
      </MoleculeStageProvider>
    </>
  );
};

export default HomePage;
