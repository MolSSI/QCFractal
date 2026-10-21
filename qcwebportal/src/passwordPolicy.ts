// Client-side mirror of the server's new-password policy (qcportal is_valid_password).
// This is only for giving immediate feedback; the server enforces the real policy.

export const MIN_PASSWORD_LENGTH = 12;
export const MAX_PASSWORD_BYTES = 72;

// Returns an error message if the password is not an acceptable new password, otherwise undefined.
export function validateNewPassword(password: string): string | undefined {
  if (password.length < MIN_PASSWORD_LENGTH) {
    return `Password must be at least ${MIN_PASSWORD_LENGTH} characters`;
  }
  if (new TextEncoder().encode(password).length > MAX_PASSWORD_BYTES) {
    return `Password is too long (at most ${MAX_PASSWORD_BYTES} bytes when encoded as UTF-8)`;
  }
  return undefined;
}
