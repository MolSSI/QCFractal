export const server_address = import.meta.env.VITE_QCFRACTAL_URI;

export const server_headers: Record<string, string> = {
  "Content-Type": "application/json",
  Accept: "application/json",
};
