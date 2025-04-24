import * as qcpTypes from "./PortalTypes";

const bohr_to_angstrom = 0.529177210544;

export function moleculeToSDF(qc: qcpTypes.Molecule): string {
  const natoms = qc.symbols.length;
  const lines: string[] = [];
  const nbonds = qc.connectivity?.length ?? 0;

  lines.push(qc.name ? qc.name : qc.identifiers.molecular_formula); // Name
  lines.push("  NGL Viewer"); // program
  lines.push(""); // comment

  lines.push(`${natoms.toString().padStart(3)}${nbonds.toString().padStart(3)}  0  0  0  0            999 V2000`);

  for (let i = 0; i < natoms; i++) {
    const x = (bohr_to_angstrom * qc.geometry[i * 3 + 0]).toFixed(4).padStart(10);
    const y = (bohr_to_angstrom * qc.geometry[i * 3 + 1]).toFixed(4).padStart(10);
    const z = (bohr_to_angstrom * qc.geometry[i * 3 + 2]).toFixed(4).padStart(10);
    const sym = qc.symbols[i].padEnd(3);
    lines.push(`${x}${y}${z} ${sym} 0  0  0  0  0  0  0  0  0  0  0  0`);
  }

  qc.connectivity?.forEach(bond => {
    const a1 = (bond[0] + 1).toString().padStart(3);
    const a2 = (bond[1] + 1).toString().padStart(3);
    const order = bond[2].toString().padStart(3);
    lines.push(`${a1}${a2}${order}  0  0  0  0`);
  });

  lines.push("M  END");
  lines.push("$$$$");
  return lines.join("\n");
}