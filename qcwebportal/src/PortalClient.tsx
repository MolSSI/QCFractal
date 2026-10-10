import { createContext, ReactNode, useCallback, useContext } from "react";
import * as requestHelpers from "./RequestHelpers.ts";
import { AuthenticationError, AuthorizationError } from "./Exceptions.ts";
import { useAuth } from "./Auth.tsx";

type ClientContextType = {
  makeRequest: <T>(
    method: string,
    endpoint: string,
    body?: object | FormData,
    url_params?: Record<string, string | string[]>,
  ) => Promise<T>;
};

export const PortalClientContext = createContext<ClientContextType | undefined>(
  undefined,
);

export function PortalClientProvider({ children }: { children: ReactNode }) {
  const { ping } = useAuth();

  const wrappedMakeRequest = useCallback(
    async function <T>(
      method: string,
      endpoint: string,
      body?: object | FormData,
      url_params?: Record<string, string | string[]>,
    ): Promise<T> {
      try {
        return await requestHelpers.rawMakeRequest<T>(
          method,
          endpoint,
          body,
          url_params,
        );
      } catch (err) {
        const errmsg = `Failed to request data: ${err instanceof Error ? err.message : String(err)}`;

        // A 403 can mean the session lapsed just as much as a 401 does: the
        // server sees no role while the client still holds the old user info.
        // Re-ping either way so the UI stops offering actions that will fail.
        if (
          err instanceof AuthenticationError ||
          err instanceof AuthorizationError
        ) {
          await ping();

          // Rethrow the same kind of error so callers can tell an auth
          // failure from an ordinary one and explain it in plain language
          throw err instanceof AuthenticationError
            ? new AuthenticationError(errmsg)
            : new AuthorizationError(errmsg);
        }

        throw new Error(errmsg);
      }
    },
    [ping],
  );

  return (
    <PortalClientContext.Provider
      value={{
        makeRequest: wrappedMakeRequest,
      }}
    >
      {children}
    </PortalClientContext.Provider>
  );
}

export function usePortalClient() {
  const context = useContext(PortalClientContext);
  if (!context) {
    throw new Error(
      "usePortalClientRequest must be used within a PortalClientProvider",
    );
  }

  const { makeRequest } = context;
  return { makeRequest };
}
