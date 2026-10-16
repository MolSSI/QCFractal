import { defineConfig, loadEnv, type ProxyOptions } from "vite";
import react from "@vitejs/plugin-react";

// Proxies requests for the QCFractal server through the dev server, so that to the
// browser the server appears to be on the same origin as the portal
function qcfractalProxy(target: string): ProxyOptions {
  const targetOrigin = new URL(target).origin;

  return {
    target,
    // Remote servers (behind their own proxies) may route on the Host header
    changeOrigin: true,
    // Keep the session cookie on the dev server's host
    cookieDomainRewrite: "",
    configure: (proxy) => {
      proxy.on("proxyReq", (proxyReq, req) => {
        // The server rejects state-changing requests whose Origin isn't the server itself (CSRF protection).
        // Requests from the portal on this dev server are made to look like they come from the server's origin.
        // Requests from any other origin are passed through unchanged, so the server still rejects them.
        if (req.headers.origin === `http://${req.headers.host}`) {
          proxyReq.setHeader("origin", targetOrigin);
        }
      });
    },
  };
}

// https://vite.dev/config/
export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, process.cwd(), "");

  // When running the dev server, requests to VITE_QCFRACTAL_URI go through a proxy, unless
  // QCFRACTAL_DEV_DIRECT is set (in which case the server must allow the dev server's origin via CORS)
  const devProxyTarget =
    command === "serve" && env.VITE_QCFRACTAL_URI && !env.QCFRACTAL_DEV_DIRECT
      ? env.VITE_QCFRACTAL_URI
      : "";

  return {
    plugins: [react()],
    define: {
      __QCFRACTAL_DEV_PROXY_TARGET__: JSON.stringify(devProxyTarget),
    },
    server: devProxyTarget
      ? {
          proxy: {
            "/api/": qcfractalProxy(devProxyTarget),
            "/auth/": qcfractalProxy(devProxyTarget),
          },
        }
      : undefined,
  };
});
