export class AuthorizationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AuthorizationError";
  }
}

export class AuthenticationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AuthenticationError";
  }
}

export class RateLimitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RateLimitError";
  }
}

// Login succeeded, but the browser did not keep the session cookie the server sent
export class SessionCookieBlockedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SessionCookieBlockedError";
  }
}
