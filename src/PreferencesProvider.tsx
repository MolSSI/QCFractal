import React, { createContext, useContext, ReactNode, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { usePortalClient } from "./PortalClient.tsx";
import { useAuth } from "./Auth.tsx";
import * as qcpTypes from "./PortalTypes.ts";

interface PreferencesContextType {
  preferences: qcpTypes.UserPreferences | undefined;
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
  setPreferences: (prefs: qcpTypes.UserPreferences) => Promise<void>;
  updatePreference: (key: string, value: unknown) => Promise<void>;
}

const PreferencesContext = createContext<PreferencesContextType | undefined>(undefined);

export const PreferencesProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const { makeRequest } = usePortalClient();
  const { userInfo, authorized } = useAuth();
  const queryClient = useQueryClient();

  const {
    data: preferences,
    isLoading,
    isError,
    error,
  } = useQuery({
    queryKey: ["userPreferences"],
    queryFn: () => makeRequest<qcpTypes.UserPreferences>("GET", "/api/v1/me/preferences"),
    enabled: authorized && !!userInfo,
  });

  const setPrefsMutation = useMutation({
    mutationFn: (newPrefs: qcpTypes.UserPreferences) => {
      if (!userInfo) {
        throw new Error("User not logged in");
      }
      return makeRequest<void>(
        "PUT",
        `/api/v1/me/preferences`,
        newPrefs
      );
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["userPreferences"] });
    },
  });

  const setPreferences = useCallback(
    async (prefs: qcpTypes.UserPreferences) => {
      await setPrefsMutation.mutateAsync(prefs);
    },
    [setPrefsMutation]
  );

  const updatePreference = useCallback(
    async (key: string, value: unknown) => {
      // Per instructions: download whole thing, update key, upload again
      const currentPrefs = await makeRequest<qcpTypes.UserPreferences>("GET", "/api/v1/me/preferences");
      const updatedPrefs = { ...currentPrefs, [key]: value };
      await setPrefsMutation.mutateAsync(updatedPrefs);
    },
    [makeRequest, setPrefsMutation]
  );

  return (
    <PreferencesContext.Provider
      value={{
        preferences,
        isLoading,
        isError,
        error: error as Error | null,
        setPreferences,
        updatePreference,
      }}
    >
      {children}
    </PreferencesContext.Provider>
  );
};

export const usePreferences = () => {
  const context = useContext(PreferencesContext);
  if (context === undefined) {
    throw new Error("usePreferences must be used within a PreferencesProvider");
  }
  return context;
};
