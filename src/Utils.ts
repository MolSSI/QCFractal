export const parseToDate = (isoString?: string): Date | undefined => {
  if (!isoString) {
    return undefined
  }

  // Trim to milliseconds (3 digits after the dot)
  const trimmed = isoString.replace(/(\.\d{3})\d+/, "$1");
  return new Date(trimmed);
};