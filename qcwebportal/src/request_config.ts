export const server_address = import.meta.env.VITE_QCFRACTAL_URI;

// The server requires this header on every state-changing request authenticated
// by the session cookie (CSRF protection). Its value is not checked, only its presence.
export const csrf_headers: Record<string, string> = {
  "X-Requested-With": "XMLHttpRequest",
};

export const server_headers: Record<string, string> = {
  "Content-Type": "application/json",
  Accept: "application/json",
  ...csrf_headers,
};
