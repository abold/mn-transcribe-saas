"use client";

import { useEffect, useRef, useState } from "react";
import { uploadFile, createJob, getJobWithRetry, createCheckout } from "@/lib/api";
import type { Session } from "@supabase/supabase-js";
import { supabase } from "@/lib/supabaseClient";
import PricingSection from "@/components/PricingSection";

export default function Home() {
  // ----------------------------
  // Auth state
  // ----------------------------
  const [session, setSession] = useState<Session | null>(null);
  const [authEmail, setAuthEmail] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const [authMsg, setAuthMsg] = useState<string>("");

  // ----------------------------
  // Your existing app state
  // ----------------------------
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<string>("Bereit");
  const [jobId, setJobId] = useState<string>("");
  const [srtUrl, setSrtUrl] = useState<string>("");
  const [fileUrl, setFileUrl] = useState<string>("");
  const [progress, setProgress] = useState<number>(0);
  const [busy, setBusy] = useState<boolean>(false);
  const [error, setError] = useState<string>("");

  // ✅ for canceling upload + polling
  const uploadAbort = useRef<AbortController | null>(null);
  const pollRef = useRef<number | null>(null);
  const pollAbort = useRef<AbortController | null>(null);

  function clearPoll() {
    if (pollRef.current) {
      window.clearInterval(pollRef.current);
      pollRef.current = null;
    }
    pollAbort.current?.abort();
    pollAbort.current = null;
  }

  // ----------------------------
  // Auth: load session + listen changes
  // ----------------------------
  useEffect(() => {
    if (!supabase) {
      setAuthMsg("Supabase env vars missing (check Vercel env + redeploy).");
      return;
    }

    supabase.auth.getSession().then(({ data }) => {
      setSession(data.session ?? null);
    });

    const { data: sub } = supabase.auth.onAuthStateChange((_event, newSession) => {
      setSession(newSession);
    });

    return () => {
      sub.subscription.unsubscribe();
    };
  }, []);

  // ----------------------------
  // Magic link login
  // ----------------------------
  async function sendMagicLink() {
    if (!supabase) {
      setAuthMsg("Supabase not configured.");
      return;
    }
    const email = authEmail.trim();
    if (!email) {
      setAuthMsg("Please enter your email.");
      return;
    }

    setAuthBusy(true);
    setAuthMsg("");

    try {
      const redirectTo = `${window.location.origin}/auth/callback`;

      const { error } = await supabase.auth.signInWithOtp({
        email,
        options: { emailRedirectTo: redirectTo },
      });

      if (error) throw error;

      setAuthMsg("✅ Magic link sent. Check your inbox (and spam).");
    } catch (e: any) {
      setAuthMsg(`❌ Login failed: ${e?.message || "unknown error"}`);
    } finally {
      setAuthBusy(false);
    }
  }

  async function logout() {
    if (!supabase) return;
    await supabase.auth.signOut();
    setSession(null);

    // reset UI bits if you want
    setFile(null);
    setStatus("Bereit");
    setJobId("");
    setSrtUrl("");
    setFileUrl("");
    setProgress(0);
    setBusy(false);
    setError("");
  }

  // ----------------------------
  // Stripe checkout buttons (optional UI)
  // ----------------------------
  async function goCheckout(plan: "creator" | "pro") {
    try {
      setError("");
      setStatus("Redirecting to checkout…");
      const { url } = await createCheckout(plan);
      window.location.href = url;
    } catch (e: any) {
      const msg = e?.message || "Checkout failed";
      setError(msg);
      setStatus(msg);
    }
  }

  // ----------------------------
  // Your existing start() unchanged,
  // but we enforce login before allowing it.
  // ----------------------------
  async function start() {
    console.log("START CLICKED ✅ build:", "2025-12-14-DEPLOY-1");

    if (!session) {
      setError("Please log in first.");
      return;
    }
    if (!file) return;

    setBusy(true);
    setError("");
    setStatus("Presign + Upload…");
    setSrtUrl("");
    setFileUrl("");
    setJobId("");
    setProgress(0);

    // abort controllers
    uploadAbort.current?.abort();
    uploadAbort.current = new AbortController();

    try {
      // 1) presign + PUT upload (inside api.ts)
      const ps = await uploadFile(file, uploadAbort.current.signal);
      console.log("uploaded:", ps);
      setProgress(100);

      // 2) create job
      setStatus("Job anlegen…");

      const fk = (ps.file_key ?? ps.key)?.trim();
      if (!fk) {
        throw new Error("Upload succeeded but file_key/key is missing");
      }

      console.log("createJob file_key =", fk); // <-- TEMP debug, safe to keep
      const job = await createJob(fk);

      setJobId(job.id);
      setStatus(job.status);

      // 3) polling
      clearPoll();
      pollAbort.current = new AbortController();

      pollRef.current = window.setInterval(async () => {
        try {
          const data = await getJobWithRetry(job.id, {
            maxRetries: 3,
            timeoutMs: 5000,
            signal: pollAbort.current!.signal,
          });

          setStatus(data.status);

          if (data.status === "failed") {
            clearPoll();
            setError(data.error_msg || "Job failed");
            setBusy(false);
            return;
          }

          if (data.status === "done") {
            clearPoll();
            if (data.srt_url) setSrtUrl(data.srt_url);
            if (data.file_url) setFileUrl(data.file_url);
            setBusy(false);
            return;
          }
        } catch (e: unknown) {
          if (pollAbort.current?.signal.aborted) return;

          clearPoll();
          setBusy(false);

          const msg =
            e instanceof Error ? e.message : typeof e === "string" ? e : "Fehler";

          setStatus(msg);
          setError(msg);
        }
      }, 1500);
    } catch (e: unknown) {
      setBusy(false);

      const msg =
        e instanceof Error ? e.message : typeof e === "string" ? e : "Fehler";

      setStatus(msg);
      setError(msg);
    }
  }

  function cancelUpload() {
    // ✅ cancel upload
    uploadAbort.current?.abort();
    uploadAbort.current = null;

    // ✅ cancel poll
    clearPoll();

    setBusy(false);
    setStatus("Abgebrochen");
  }

  useEffect(() => {
    return () => {
      uploadAbort.current?.abort();
      clearPoll();
    };
  }, []);

  // ----------------------------
  // UI
  // ----------------------------
  return (
    <main className="min-h-screen bg-neutral-950 text-neutral-100">
      <div className="max-w-3xl mx-auto p-6">
        <PricingSection />
        <div className="mt-10"></div>
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold mb-2">🎙️ Mongolian Transcribe</h1>
            <p className="text-neutral-400 mb-6">
              Datei auswählen → Upload → Job starten → SRT herunterladen
            </p>
          </div>

          {/* Auth box */}
          <div className="min-w-[280px] rounded-lg border border-neutral-800 bg-neutral-900/40 p-4">
            {session ? (
              <div className="space-y-3">
                <div className="text-sm text-neutral-300">
                  Logged in as:
                  <div className="font-mono break-all text-neutral-100">
                    {session.user.email}
                  </div>
                </div>

                <div className="flex gap-2">
                  <button
                    onClick={() => goCheckout("creator")}
                    className="px-3 py-2 rounded-md bg-blue-600 hover:bg-blue-700 text-sm"
                  >
                    Upgrade Creator
                  </button>
                  <button
                    onClick={() => goCheckout("pro")}
                    className="px-3 py-2 rounded-md bg-purple-600 hover:bg-purple-700 text-sm"
                  >
                    Upgrade Pro
                  </button>
                </div>

                <button
                  onClick={logout}
                  className="w-full px-3 py-2 rounded-md bg-neutral-800 hover:bg-neutral-700 text-sm"
                >
                  Logout
                </button>
              </div>
            ) : (
              <div className="space-y-3">
                <div className="text-sm text-neutral-300 font-medium">Login</div>

                <input
                  value={authEmail}
                  onChange={(e) => setAuthEmail(e.target.value)}
                  placeholder="you@example.com"
                  className="w-full px-3 py-2 rounded-md bg-neutral-950 border border-neutral-800 text-sm outline-none"
                />

                <button
                  onClick={sendMagicLink}
                  disabled={authBusy}
                  className="w-full px-3 py-2 rounded-md bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-sm"
                >
                  {authBusy ? "Sending…" : "Send magic link"}
                </button>

                {authMsg ? (
                  <div className="text-xs text-neutral-300 whitespace-pre-wrap">
                    {authMsg}
                  </div>
                ) : (
                  <div className="text-xs text-neutral-500">
                    We’ll email you a sign-in link.
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Main app (requires login) */}
        {!session ? (
          <div className="mt-6 rounded-lg border border-neutral-800 bg-neutral-900/40 p-4 text-sm text-neutral-300">
            🔒 Please log in to start transcription (needed to enforce free limits and attach subscriptions).
          </div>
        ) : null}
        <div id="uploader">
          <label className="block mb-4 mt-6">
            <input
              type="file"
              accept="video/*,audio/*"
              disabled={!session}
              className="block w-full text-sm disabled:opacity-50 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:bg-blue-600 file:text-white hover:file:bg-blue-700"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
            />
          </label>

          <div className="flex gap-2 mb-3">
            <button
              onClick={start}
              disabled={!session || !file || busy}
              className="px-4 py-2 rounded-md bg-blue-600 hover:bg-blue-700 disabled:opacity-50"
            >
              {busy ? "Bitte warten…" : "Upload & Transcribe"}
            </button>

            <button
              onClick={cancelUpload}
              disabled={!busy}
              className="px-4 py-2 rounded-md bg-neutral-800 hover:bg-neutral-700 disabled:opacity-50"
            >
              Abbrechen
            </button>
          </div>

          <div className="mb-2 text-sm">
            <span className="text-neutral-400">Status:</span>{" "}
            <span className="font-mono">{status}</span>
          </div>
          
          {error && <div className="text-red-400 text-sm mb-2">{error}</div>}

          <progress value={progress} max={100} className="w-full h-2 rounded mb-4" />

          {/* Player */}
          {fileUrl ? (
            <div className="mt-4">
              <video className="w-full rounded bg-black" controls src={fileUrl}>
                {srtUrl && (
                  <track label="MN" kind="subtitles" srcLang="mn" src={srtUrl} default />
                )}
              </video>
            </div>
          ) : null}

          <div className="grid grid-cols-4 gap-2 text-sm mt-4">
            <div className="text-neutral-400">Job ID</div>
            <div className="col-span-3 font-mono break-all">{jobId || "—"}</div>

            <div className="text-neutral-400">Download</div>
            <div className="col-span-3">
              {srtUrl ? (
                <a className="text-blue-400 underline break-all" href={srtUrl} target="_blank">
                  ⬇️ SRT herunterladen
                </a>
              ) : (
                "—"
              )}
            </div>
          </div>
        </div>
      </div>
    </main>
  );
}

