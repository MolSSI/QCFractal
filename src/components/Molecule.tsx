import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { moleculeToSDF } from "../MoleculeUtils";
import * as qcpTypes from "../PortalTypes";
import { Stage } from "ngl";

const StageContext = createContext<Stage | undefined>(undefined);

function MoleculeStageProvider({ width, height, children }) {
  const stageElementRef = useRef<HTMLDivElement>(null);
  const stageRef = useRef<Stage | null>(null);
  const [isReady, setIsReady] = useState(false);

  useEffect(() => {
    if (stageElementRef.current && !stageRef.current) {
      stageRef.current = new Stage(stageElementRef.current, {
        backgroundColor: "white",
      });
      setIsReady(true);

      const handleResize = () => stageRef.current.handleResize();
      window.addEventListener("resize", handleResize);
      return () => {
        window.removeEventListener("resize", handleResize);
        stageRef.current.dispose();
      };
    }
  }, []);

  return (
    <>
      <div ref={stageElementRef} style={{ width, height }} />
      {isReady && (
        <StageContext.Provider value={stageRef.current}>
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
  const componentRef = useRef<any>(null);

  useEffect(() => {
    // Remove previous component if it exists
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
        componentRef.current = comp;
        comp.addRepresentation("ball+stick", { multipleBond: true });
        stage.autoView();
      });
  }, [stage, moleculeData]);

  // Stage is rendered in the provider
  return null;
};

export { MoleculeStageProvider, MoleculeViewer };
