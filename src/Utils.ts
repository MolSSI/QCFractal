export const parseToDate = (isoString?: string): Date | undefined => {
  if (!isoString) {
    return undefined;
  }

  // Trim to milliseconds (3 digits after the dot)
  const trimmed = isoString.replace(/(\.\d{3})\d+/, "$1");
  return new Date(trimmed);
};

export const updateFavoritesList = (existing_favorites: number[] | undefined, proj_id: number): number[] => {
  // adds or removes the new_id to/from the existing_favorites
  // Also handles if existing favorites is undefined
  if (!existing_favorites) return [proj_id];

  // If the project is already in the list, remove it
  if (existing_favorites.includes(proj_id)) {
    return existing_favorites.filter((id) => id !== proj_id);
  }
  return [...existing_favorites, proj_id];
};