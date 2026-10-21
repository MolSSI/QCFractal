import { useAuth } from "../Auth.tsx";
import { usePreferences } from "../PreferencesProvider.tsx";
import { IconButton, Tooltip } from "@mui/material";
import { Star, StarBorder } from "@mui/icons-material";
import React from "react";

export const updateFavoritesList = (
  existing_favorites: number[] | undefined,
  obj_id: number,
): number[] => {
  // adds or removes the new_id to/from the existing_favorites
  // Also handles if existing favorites is undefined
  if (!existing_favorites) return [obj_id];

  // If the project is already in the list, remove it
  if (existing_favorites.includes(obj_id)) {
    return existing_favorites.filter((id) => id !== obj_id);
  }
  return [...existing_favorites, obj_id];
};

interface FavoriteButtonProps {
  objectId: number;
  preferencesKey: string;
}

export const FavoriteButton: React.FC<FavoriteButtonProps> = ({
  objectId,
  preferencesKey,
}) => {
  const { has_permission } = useAuth();
  const { preferences, updatePreference } = usePreferences();

  const canFavorite = has_permission("me", "modify");
  if (!canFavorite) return null;

  const existingFavorites = (preferences && preferencesKey in preferences)
    ? (preferences[preferencesKey] as number[] | [])
    : [];
  const isFavorite = existingFavorites.includes(objectId);

  const handleToggleFavorite = async () => {
    const newFavorites = updateFavoritesList(existingFavorites, objectId);
    await updatePreference(preferencesKey, newFavorites);
  };

  return (
    <Tooltip title={isFavorite ? "Remove from favorites" : "Add to favorites"}>
      <IconButton
        size="small"
        onClick={handleToggleFavorite}
        sx={{ mr: 1, p: 1 }}
      >
        {isFavorite ? (
          <Star sx={{ color: "gold", fontSize: "1.5rem" }} />
        ) : (
          <StarBorder sx={{ fontSize: "1.5rem" }} />
        )}
      </IconButton>
    </Tooltip>
  );
};