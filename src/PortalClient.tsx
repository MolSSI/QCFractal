import { createContext, ReactNode, useCallback, useContext } from "react";
import * as requestHelpers from "./RequestHelpers.ts";
import { AuthenticationError } from "./Exceptions.ts";
import { useAuth } from "./Auth.tsx";

export type RequestReturnType<T> = {
  data?: T;
  error?: string;
};

export type FetchedData<T> = {
  data?: T;
  error?: string;
  loading: boolean;
};

type ClientContextType = {
  makeRequest: <T>(
    method: string,
    endpoint: string,
    body?: object,
    url_params?: Record<string, string>,
  ) => Promise<RequestReturnType<T>>;

  fetchData: <T>(
    setDataFn: (value: FetchedData<T>) => void,
    method: string,
    endpoint: string,
    body?: object,
    url_params?: Record<string, string>,
  ) => void;
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
    ): Promise<RequestReturnType<T>> {
      try {
        const r = await requestHelpers.rawMakeRequest<T>(
          method,
          endpoint,
          body,
          url_params,
        );
        return { data: r, error: undefined };
      } catch (err) {
        if (err instanceof AuthenticationError) {
          await ping();
        }
        const errmsg = `Failed to request data: ${err instanceof Error ? err.message : String(err)}`;
        return { data: undefined, error: errmsg } as RequestReturnType<T>;
      }
    },
    [ping],
  );

  const fetchData = useCallback(
    function <T>(
      setDataFn: (value: FetchedData<T>) => void,
      method: string,
      endpoint: string,
      body?: object,
      url_params?: Record<string, string>,
    ): void {
      setDataFn({ data: undefined, error: undefined, loading: true });

      wrappedMakeRequest<T>(method, endpoint, body, url_params)
        .then((r) => {
          setDataFn((prevData) => ({
            ...prevData,
            data: r.data,
            error: r.error,
          }));
        })
        .catch((err) => {
          setDataFn((prevData) => ({
            ...prevData,
            data: undefined,
            error: err,
          }));
        })
        .finally(() => {
          setDataFn((prevData) => ({ ...prevData, loading: false }));
        });
    },
    [wrappedMakeRequest],
  );

  return (
    <PortalClientContext.Provider
      value={{
        makeRequest: wrappedMakeRequest,
        fetchData: fetchData,
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

  const { makeRequest, fetchData } = context;
  return { makeRequest, fetchData };
}
