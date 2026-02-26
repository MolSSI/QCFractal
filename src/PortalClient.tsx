import { createContext, ReactNode, useCallback, useContext } from "react";
import * as requestHelpers from "./RequestHelpers.ts";
import { AuthenticationError } from "./Exceptions.ts";
import { useAuth } from "./Auth.tsx";

type ClientContextType = {
  makeRequest: <T>(
    method: string,
    endpoint: string,
    body?: object,
    url_params?: Record<string, string>,
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
      body?: object,
      url_params?: Record<string, string>,
    ): Promise<T> {
      try {
        return await requestHelpers.rawMakeRequest<T>(
          method,
          endpoint,
          body,
          url_params,
        );
      } catch (err) {
        if (err instanceof AuthenticationError) {
          // Ping the server to see if the user is still logged in and that it is still up
          await ping();
        }
        const errmsg = `Failed to request data: ${err instanceof Error ? err.message : String(err)}`;
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
