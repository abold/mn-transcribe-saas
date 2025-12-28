const apiBase = () => {
  const env =
    process.env.NEXT_PUBLIC_API_BASE ||
    process.env.NEXT_PUBLIC_API_URL;

  if (env) return env.replace(/\/$/, "");

  // Fallback (local dev only)
  if (typeof window !== "undefined")
    return `${window.location.protocol}//${window.location.hostname}:8000`;

  return "http://localhost:8000";
};

// ---- Types ----
export type PresignResp = {
  key: string;
  file_key: string; // object_key
  url: string;      // presigned PUT
  read_url?: string; // (optional) older minio direct url
};

export type JobStatus = "queued" | "processing" | "done" | "failed";

export type JobView = {
  id: string;
  status: JobStatus;
  file_key?: string | null;
  duration_sec?: number | null;
  srt_key?: string | null;
  error_msg?: string | null;
  file_url?: string | null;  // presigned GET
  srt_url?: string | null;   // presigned GET
};

// ---- Small helpers ----
async function jsonOrText(r: Response) {
  const t = await r.text();
  try { return JSON.parse(t); } catch { return t; }
}

function httpError(where: string, r: Response, body: unknown): never {
  const suffix = typeof body === "string" && body ? `: ${body}` : "";
  throw new Error(`${where} HTTP ${r.status}${suffix}`);
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

// Exponential Backoff + Jitter (bis maxMs)
function backoffDelay(attempt: number, baseMs = 300, maxMs = 2500) {
  const exp = Math.min(maxMs, baseMs * Math.pow(2, attempt));
  const jitter = Math.random() * 0.3 * exp; // bis +30% drauf
  return Math.min(maxMs, Math.floor(exp + jitter));
}

// Fetch mit Timeout + optional externer Abort
async function fetchWithTimeout(
  input: RequestInfo | URL,
  init: RequestInit = {},
  timeoutMs = 5000,
  externalSignal?: AbortSignal
) {
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), timeoutMs);
  const onAbort = () => ctrl.abort();
  externalSignal?.addEventListener("abort", onAbort, { once: true });

  try {
    return await fetch(input, { ...init, signal: ctrl.signal });
  } finally {
    clearTimeout(to);
    externalSignal?.removeEventListener("abort", onAbort);
  }
}

// ---- API calls ----
export async function presign(filename: string, contentType?: string): Promise<PresignResp> {
  const r = await fetch(`${apiBase()}/v1/presign`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify({
      filename,
      content_type: contentType || "application/octet-stream",
    }),
  });
  if (!r.ok) {
    const body = await jsonOrText(r);
    httpError("presign", r, body);
  }
  return r.json();
}

/**
 * Upload a File to the presigned PUT url.
 * IMPORTANT: Content-Type must match the one used when signing.
 */
export async function uploadToPresignedUrl(
  file: File,
  presigned: PresignResp,
  signal?: AbortSignal
): Promise<void> {
  const contentType = file.type || "application/octet-stream";

  const r = await fetch(presigned.url, {
    method: "PUT",
    headers: {
      "Content-Type": contentType,
    },
    body: file,
    signal,
  });

  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`upload PUT failed: HTTP ${r.status} ${body}`);
  }
}

/**
 * Convenience helper: presign + PUT upload.
 * Returns the PresignResp (contains file_key).
 */
export async function uploadFile(file: File, signal?: AbortSignal): Promise<PresignResp> {
  const contentType = file.type || "application/octet-stream";
  const p = await presign(file.name, contentType);
  await uploadToPresignedUrl(file, p, signal);
  return p;
}

export async function createJob(file_key: string): Promise<{ id: string; status: JobStatus }> {
  const r = await fetch(`${apiBase()}/v1/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify({ file_key }), // ✅ remove engine
  });
  if (!r.ok) {
    const body = await jsonOrText(r);
    httpError("createJob", r, body);
  }
  return r.json();
}

export async function getJob(id: string, signal?: AbortSignal): Promise<JobView> {
  const r = await fetchWithTimeout(
    `${apiBase()}/v1/jobs/${id}`,
    { method: "GET", headers: { "Accept": "application/json" }, cache: "no-store" },
    5000,
    signal
  );
  if (!r.ok) {
    const body = await jsonOrText(r);
    httpError("getJob", r, body);
  }
  return r.json();
}

// Mit Retry (Exponential Backoff + Jitter)
export async function getJobWithRetry(
  id: string,
  opts?: { maxRetries?: number; backoffBaseMs?: number; timeoutMs?: number; signal?: AbortSignal }
): Promise<JobView> {
  const { maxRetries = 60, backoffBaseMs = 300, timeoutMs = 8000, signal } = opts || {};
  let attempt = 0;

  while (true) {
    try {
      const r = await fetchWithTimeout(
        `${apiBase()}/v1/jobs/${id}`,
        { method: "GET", headers: { "Accept": "application/json" }, cache: "no-store" },
        timeoutMs,
        signal
      );
      if (!r.ok) {
        const body = await jsonOrText(r);
        httpError("getJobWithRetry", r, body);
      }
      return r.json();
    } catch (err) {
      if (signal?.aborted) throw err;
      if (attempt >= maxRetries) throw err;
      await sleep(backoffDelay(attempt, backoffBaseMs));
      attempt += 1;
    }
  }
}
export async function createCheckout(plan: "creator" | "pro") {
  const res = await fetch(
    `${process.env.NEXT_PUBLIC_API_BASE}/v1/billing/checkout`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ plan }),
    }
  );

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Checkout failed");
  }

  return res.json(); // { url }
}
