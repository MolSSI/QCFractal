import * as qcpTypes from "./PortalTypes";
import { AuthenticationError, AuthorizationError } from "./Exceptions.ts";

/**
 * Turns a failed request into something worth showing a user. Auth failures
 * get an explanation and a way forward instead of the raw server message,
 * which names roles and actions that mean nothing outside the API.
 */
export function describeRequestError(error: unknown, fallback: string): string {
  if (error instanceof AuthenticationError) {
    return "Your session has expired. Please log in again and retry.";
  }

  if (error instanceof AuthorizationError) {
    return "You are not authorized to do this. Your session may have expired — try logging in again, or ask an administrator for access.";
  }

  return error instanceof Error && error.message ? error.message : fallback;
}

export const parseToDate = (isoString?: string): Date | undefined => {
  if (!isoString) {
    return undefined;
  }

  // Trim to milliseconds (3 digits after the dot)
  const trimmed = isoString.replace(/(\.\d{3})\d+/, "$1");
  return new Date(trimmed);
};

export const dateStringToLocalTime = (
  isoString: string | null | undefined,
): string | undefined => {
  if (!isoString) {
    return undefined;
  }

  const date = new Date(isoString);
  return date.toLocaleString(undefined, {
    timeZoneName: "short",
  });
};

export const truncateFront = (s: string, maxLength: number): string => {
  return s.length > maxLength
    ? "..." + s.slice(s.length - maxLength, s.length)
    : s;
};

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

/**
 * Given a number of CPU hours, return a human-friendly larger time span
 * (e.g. "3.2 days" or "1.5 years") chosen based on magnitude.
 * Returns undefined if the value is too small to warrant a larger unit.
 */
export function formatCpuHoursSpan(cpuHours: number): string | undefined {
  if (!Number.isFinite(cpuHours) || cpuHours < 24) {
    return undefined;
  }

  const days = cpuHours / 24;
  const years = days / 365;

  const format = (value: number, unit: string): string => {
    const rounded = value.toLocaleString(undefined, {
      maximumFractionDigits: value >= 100 ? 0 : 1,
    });
    return `${rounded} ${value === 1 ? unit : `cpu ${unit}s`}`;
  };

  return years >= 1 ? format(years, "year") : format(days, "day");
}

export function formatSize(bytes: number): string {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const sizes = ["Bytes", "KiB", "MiB", "GiB", "TiB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

/**
 * Checks if all tokens in the query string are present in the target string in the given order.
 * Case-insensitive.
 */
export function matchesTokens(
  query: string,
  target: string | null | undefined,
): boolean {
  if (!target) return false;
  if (!query) return true;

  const queryTokens = query
    .toLowerCase()
    .split(/\s+/)
    .filter((t) => t.length > 0);
  const targetLower = target.toLowerCase();

  let lastIndex = -1;
  for (const token of queryTokens) {
    const index = targetLower.indexOf(token, lastIndex + 1);
    if (index === -1) {
      return false;
    }
    lastIndex = index;
  }

  return true;
}
