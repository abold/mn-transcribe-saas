"use client";

import { useEffect, useRef, useState } from "react";
import { uploadFile, createJob, getJobWithRetry } from "@/lib/api";

export default function Home() {
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

  async function start() {
    console.log("START CLICKED ✅ build:", "2025-12-14-DEPLOY-1");

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
            e instanceof Error
              ? e.message
              : typeof e === "string"
              ? e
              : "Fehler";

          setStatus(msg);
          setError(msg);
        }
      }, 1500);
    } catch (e: unknown) {
      setBusy(false);

      const msg =
        e instanceof Error
          ? e.message
          : typeof e === "string"
          ? e
          : "Fehler";

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

  return (
    <main className="min-h-screen bg-neutral-950 text-neutral-100">
      <div className="max-w-3xl mx-auto p-6">
        <h1 className="text-2xl font-semibold mb-2">🎙️ Mongolian Transcribe</h1>
        <p className="text-neutral-400 mb-6">
          Datei auswählen → Upload → Job starten → SRT herunterladen
        </p>

        <label className="block mb-4">
          <input
            type="file"
            accept="video/*,audio/*"
            className="block w-full text-sm file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:bg-blue-600 file:text-white hover:file:bg-blue-700"
            onChange={(e) => setFile(e.target.files?.[0] || null)}
          />
        </label>

        <div className="flex gap-2 mb-3">
          <button
            onClick={start}
            disabled={!file || busy}
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
    </main>
  );
}
