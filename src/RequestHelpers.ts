import * as qcpExceptions from "./Exceptions";
import { csrf_headers, server_address, server_headers } from "./request_config";

function objectToQueryParams(obj: Record<string, unknown>): string {
  const params = new URLSearchParams();

  Object.entries(obj).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((v) => {
        if (v !== undefined && v !== null) {
          params.append(key, String(v));
        }
      });
    } else if (value !== undefined && value !== null) {
      params.append(key, String(value));
    }
  });

  return params.toString();
}

export async function rawRequest<T>(
  full_url: string,
  req_options: RequestInit,
): Promise<T> {
  // Sends a request to the given URL and options, handles errors, and returns the results
  // Handles appropriate errors. Will throw:
  //     AuthenticationError if the user is not logged in and needs to be
  //     AuthorizationError if the user is logged in, but can't access the resource
  //     Error on other errors

  const response = await fetch(full_url, req_options);
  const content_type = response.headers.get("Content-Type");
  const is_json_response = content_type?.includes("application/json");

  if (response.ok) {
    if (response.status === 204) {
      return undefined as T;
    }

    if (!is_json_response) {
      const content_length = response.headers.get("Content-Length");
      if (content_length === "0") {
        return undefined as T;
      }

      throw new Error(
        `Received ok response but not JSON? - (${response.status}): ${response.statusText}`,
      );
    }
    return (await response.json()) as T;
  } else {
    // error was sent. If it's JSON, there should be a 'msg' field with a description
    if (is_json_response) {
      const rjson = await response.json();
      const message = "msg" in rjson ? (rjson.msg as string) : "(no message)";

      if (response.status === 401) {
        // 401 will mean the user is no longer authenticated
        throw new qcpExceptions.AuthenticationError(
          `API request failed (401 ${response.statusText}) - ${message}`,
        );
      } else if (response.status === 403) {
        throw new qcpExceptions.AuthorizationError(
          `API request failed (403 ${response.statusText}) - ${message}`,
        );
      } else {
        throw new Error(
          `API request failed (${response.status}): ${response.statusText} - ${rjson.msg}`,
        );
      }
    } else {
      throw new Error(
        `API request failed with unexpected response type (${response.status}): ${response.statusText}`,
      );
    }
  }
}

export async function rawMakeRequest<T>(
  method: string,
  endpoint: string,
  body?: object | FormData,
  url_params?: Record<string, string | string[]>,
): Promise<T> {
  const isFormData = body instanceof FormData;
  const req_options: RequestInit = {
    method,
    // For FormData, omit Content-Type so the browser sets it with the
    // multipart boundary. Keep Accept so we still expect a JSON response,
    // and the CSRF header so the server accepts the request.
    headers: isFormData
      ? { Accept: "application/json", ...csrf_headers }
      : server_headers,
    credentials: "include",
    body: isFormData ? body : body ? JSON.stringify(body) : undefined,
  };

  // Put URL params at the end of the url
  const base = server_address.replace(/\/+$/, "");
  const path = endpoint.replace(/^\/+/, "");
  const full_url = url_params
    ? `${base}/${path}?${objectToQueryParams(url_params)}`
    : `${base}/${path}`;

  return rawRequest<T>(full_url, req_options);
}
