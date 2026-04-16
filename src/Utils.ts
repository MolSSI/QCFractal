import * as qcpTypes from "./PortalTypes";

export const parseToDate = (isoString?: string): Date | undefined => {
  if (!isoString) {
    return undefined;
  }

  // Trim to milliseconds (3 digits after the dot)
  const trimmed = isoString.replace(/(\.\d{3})\d+/, "$1");
  return new Date(trimmed);
};

export const truncateFront = (s: string, maxLength: number): string => {
  return s.length > maxLength ? "..."+s.slice(s.length-maxLength, s.length) : s;

}

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

export const calculateTotalStatusCounts = (
  statusData: qcpTypes.DatasetStatus,
): Record<qcpTypes.RecordStatus, number> => {
  return Object.values(statusData).reduce(
    (acc, counts) => {
      Object.entries(counts).forEach(([status, count]) => {
        const s = status as qcpTypes.RecordStatus;
        acc[s] = (acc[s] || 0) + count;
      });
      return acc;
    },
    {} as Record<qcpTypes.RecordStatus, number>,
  );
};

export function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  return value as Record<string, unknown>;
}

export function stripAnsi(text: string): string {
  // eslint-disable-next-line no-control-regex
  return text.replace(/\x1b\[[0-9;]*m/g, "");

}

export function formatSize(bytes: number): string {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const sizes = ["Bytes", "KiB", "MiB", "GiB", "TiB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
};
