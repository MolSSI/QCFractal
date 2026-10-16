// Set by the dev server when it proxies requests to the QCFractal server (see vite.config.ts)
const dev_proxy_target: string = __QCFRACTAL_DEV_PROXY_TARGET__;

// Address of the QCFractal server to send requests to. If not specified at build time, the server is assumed
// to be on the same origin as the web portal (which is also the case when using the dev server's proxy)
export const server_address: string = dev_proxy_target
  ? ""
  : (import.meta.env.VITE_QCFRACTAL_URI ?? "");

// Address of the QCFractal server, for showing to users (such as for connecting with the python client)
export const server_display_address: string =
  dev_proxy_target || server_address || window.location.origin;

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
