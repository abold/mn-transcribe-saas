# worker.py
import os
import json
import time
import tempfile
import subprocess

from google.cloud import storage

import boto3
import redis
from botocore.client import Config
from botocore.exceptions import ClientError

from sqlalchemy import create_engine, text
from google.cloud import speech_v2

from service import extract_audio_to_wav
from google.cloud.speech_v2.types import RecognitionFeatures


# ---- ENV ----
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET        = os.getenv("S3_BUCKET", "uploads")
REDIS_URL     = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL  = os.getenv("DATABASE_URL", "postgresql+psycopg://app:app@db:5432/app")

GCP_REGION   = os.getenv("GCP_REGION", "global")
STT_LANGUAGE = os.getenv("STT_LANGUAGE", "mn-MN")
STT_MODEL    = os.getenv("STT_MODEL", "latest_long")

STORAGE_PROVIDER = os.getenv("STORAGE_PROVIDER", "minio")  # "minio" | "gcs"
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
gcs_client = storage.Client() if STORAGE_PROVIDER == "gcs" else None

QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY", "mn:q")

# ---- Clients ----
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
r = redis.from_url(REDIS_URL)

print("WORKER STARTED")
print("REDIS URL:", REDIS_URL)

try:
    print("REDIS PING:", r.ping())
except Exception as e:
    print("REDIS PING FAILED:", e)

db_engine = create_engine(DATABASE_URL, future=True)

# ---- Helpers ----
def _fail_job(job, reason: str):
    job_id = job.get("job_id")
    print("JOB FAILED:", job_id, "-", reason)
    if job_id:
        with db_engine.begin() as c:
            c.execute(text("""
              UPDATE jobs
              SET status='failed', error_msg=:r, finished_at=now()
              WHERE id=:i
            """), {"i": job_id, "r": (reason or "")[:4000]})


def _wav_duration_sec(path: str) -> float:
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", path],
        text=True
    )
    return float(json.loads(out)["format"]["duration"])


def _write_srt_from_text(text_val: str, duration_sec: float, out_path: str):
    end_ms = int(duration_sec * 1000)
    def ms2tc(ms):
        h = ms//3600000; ms%=3600000
        m = ms//60000;   ms%=60000
        s = ms//1000;    ms%=1000
        return f"{h:02}:{m:02}:{s:02},{ms:03}"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("1\n")
        f.write(f"00:00:00,000 --> {ms2tc(end_ms)}\n")
        f.write((text_val or "").strip() + "\n")

CHUNK_SEC = float(os.getenv("CHUNK_SEC", "55"))

def _cut_wav_chunk(src_wav: str, out_wav: str, start: float, dur: float):
    # PCM wav chunks, fast and reliable
    subprocess.check_call([
        "ffmpeg", "-y",
        "-ss", str(start), "-t", str(dur),
        "-i", src_wav,
        "-ac", "1", "-ar", "16000",
        out_wav
    ])

def _sec_from_duration(d):
    # Works for protobuf Duration-like objects
    return float(getattr(d, "seconds", 0)) + float(getattr(d, "nanos", 0)) * 1e-9

def _transcribe_wav_sync_words(client, recognizer, cfg, wav_path: str, chunk_start_sec: float):
    with open(wav_path, "rb") as f:
        content = f.read()

    # ---- Try with requested config (word time offsets) ----
    try:
        resp = client.recognize(
            request=speech_v2.RecognizeRequest(
                recognizer=recognizer,
                config=cfg,
                content=content,
            )
        )
    except Exception as e:
        # If Google rejects some config fields, retry with a safer config
        # (keeps word timestamps; drops anything that could be unsupported)
        print("Recognize failed with cfg, retrying with minimal cfg. Error:", repr(e))

        cfg_min = speech_v2.RecognitionConfig(
            language_codes=list(getattr(cfg, "language_codes", []) or []),
            model=getattr(cfg, "model", "") or "",
            auto_decoding_config=speech_v2.AutoDetectDecodingConfig(),
            features=RecognitionFeatures(
                enable_word_time_offsets=True,
            ),
        )

        resp = client.recognize(
            request=speech_v2.RecognizeRequest(
                recognizer=recognizer,
                config=cfg_min,
                content=content,
            )
        )

    words_out = []
    for res in getattr(resp, "results", []) or []:
        alts = getattr(res, "alternatives", []) or []
        if not alts:
            continue
        alt0 = alts[0]  # timestamps only in top alternative

        for w in getattr(alt0, "words", []) or []:
            start_off = getattr(w, "start_offset", None) or getattr(w, "start_time", None)
            end_off   = getattr(w, "end_offset", None)   or getattr(w, "end_time", None)

            start = chunk_start_sec + _sec_from_duration(start_off) if start_off is not None else chunk_start_sec
            end   = chunk_start_sec + _sec_from_duration(end_off)   if end_off is not None else chunk_start_sec

            words_out.append({"word": w.word, "start": start, "end": end})

    words_out.sort(key=lambda x: x["start"])
    return words_out


PUNCT_END = {".", "?", "!"}

def srt_timestamp(t):
    ms = int(round((t - int(t)) * 1000))
    s = int(t) % 60
    m = (int(t) // 60) % 60
    h = int(t) // 3600
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

def segment_words_to_cues(words, pause_gap=0.75, max_duration=6.0, max_chars=84):
    if not words:
        return []

    cues = []
    cur = [words[0]]

    for prev, w in zip(words, words[1:]):
        gap = w["start"] - prev["end"]
        cur_text = " ".join(x["word"] for x in cur)

        prev_ends_sentence = prev["word"] and prev["word"][-1] in PUNCT_END
        would_exceed_chars = len(cur_text) + 1 + len(w["word"]) > max_chars
        would_exceed_dur = (w["end"] - cur[0]["start"]) > max_duration

        if gap >= pause_gap or prev_ends_sentence or would_exceed_chars or would_exceed_dur:
            cues.append(cur)
            cur = [w]
        else:
            cur.append(w)

    cues.append(cur)
    return cues

def cues_to_srt(cues, line_len=42):
    def wrap_2lines(text):
        if len(text) <= line_len:
            return text
        cut = text.rfind(" ", 0, line_len)
        if cut == -1:
            cut = line_len
        return text[:cut].strip() + "\n" + text[cut:].strip()

    out = []
    for i, cue in enumerate(cues, 1):
        start = cue[0]["start"]
        end = cue[-1]["end"]
        text = " ".join(w["word"] for w in cue).strip()

        out.append(str(i))
        out.append(f"{srt_timestamp(start)} --> {srt_timestamp(end)}")
        out.append(wrap_2lines(text))
        out.append("")
    return "\n".join(out)


def process(job: dict):
    key = job["file_key"]
    raw = key
    for _ in range(2):
        if key.startswith(f"{BUCKET}/"):
            key = key[len(BUCKET)+1:]
    key = key.lstrip("/")
    print("RAW file_key:", repr(raw))
    print("NORMALIZED key:", repr(key))
    print("DEBUG Worker: bucket=", BUCKET, "key=", key, "endpoint=", S3_ENDPOINT)

    with tempfile.TemporaryDirectory() as td:
        # lokale Quell-Datei mit sinnvoller Endung
        _, ext = os.path.splitext(key)
        if not ext:
            ext = ".bin"
        src = os.path.join(td, f"input{ext}")

        if STORAGE_PROVIDER == "gcs":
            if not GCS_BUCKET:
                _fail_job(job, "GCS_BUCKET missing")
                return

            bucket = gcs_client.bucket(GCS_BUCKET)
            blob = bucket.blob(key)

            if not blob.exists():
                _fail_job(job, "gcs object not found")
                return

            blob.download_to_filename(src)

        else:
            # MinIO / S3 (local dev)
            try:
                s3.head_object(Bucket=BUCKET, Key=key)
            except ClientError as e:
                print("S3 HEAD failed:", e)
                _fail_job(job, "s3 head failed")
                return

            try:
                s3.download_file(BUCKET, key, src)
            except ClientError as e:
                print("S3 DOWNLOAD failed:", e)
                _fail_job(job, "s3 download failed")
                return

        # 2b) Debug
        try:
            size = os.path.getsize(src)
            print("DOWNLOADED SIZE:", size, "bytes")
        except Exception:
            print("DOWNLOADED SIZE: unknown")

        try:
            out = subprocess.check_output(
                ["ffprobe", "-hide_banner", "-loglevel", "error",
                 "-show_streams", "-select_streams", "a", src],
                stderr=subprocess.STDOUT, text=True
            )
            print("FFPROBE AUDIO STREAMS:\n", out or "(keine Ausgabe)")
        except subprocess.CalledProcessError as e:
            print("FFPROBE ERROR:\n", e.output)

        # 3) Audio extrahieren (dein bestehender Code)
        try:
            wav = extract_audio_to_wav(src, td)
        except Exception as e:
            print("FFMPEG failed:", e)
            stderr = getattr(e, "stderr", None)
            if stderr:
                try:
                    print("FFMPEG STDERR:\n", stderr.decode("utf-8", "ignore")
                          if isinstance(stderr, (bytes, bytearray)) else str(stderr))
                except Exception:
                    pass
            _fail_job(job, "ffmpeg failed")
            return

        # 4) Google STT v2 – EINMALIG & robust
        project = os.getenv("GCP_PROJECT_ID")
        region  = os.getenv("GCP_REGION", "global")
        lang    = os.getenv("STT_LANGUAGE", "mn-MN")
        model   = os.getenv("STT_MODEL", "latest_long")  # latest_long | latest_short

        if not project:
            _fail_job(job, "stt failed: GCP_PROJECT_ID missing")
            return

        client = speech_v2.SpeechClient()
        recognizer = f"projects/{project}/locations/{region}/recognizers/default"

        # Falls 'default' noch nicht existiert, einmalig anlegen
        try:
            client.get_recognizer(name=recognizer)
            print("Recognizer OK:", recognizer)
        except Exception as e:
            print("Recognizer missing, creating:", recognizer, "reason:", repr(e))
            try:
                from google.cloud.speech_v2.types import Recognizer
                op = client.create_recognizer(
                    parent=f"projects/{project}/locations/{region}",
                    recognizer=Recognizer(name=recognizer, language_codes=[lang])
                )
                op.result(timeout=120)
                print("Recognizer created:", recognizer)
            except Exception as ce:
                print("Recognizer create failed:", repr(ce))
                _fail_job(job, "stt failed: recognizer create/get failed")
                return

        print("STT config → project:", project, "region:", region, "lang:", lang, "model:", model, "recognizer:", recognizer)

        # ✅ Minimal supported config:
        # Keep word time offsets, drop automatic punctuation (was causing InvalidArgument)
        cfg = speech_v2.RecognitionConfig(
            language_codes=[lang],
            model=model,
            auto_decoding_config=speech_v2.AutoDetectDecodingConfig(),
            features=RecognitionFeatures(
                enable_word_time_offsets=True,
            ),
        )

        # Recognize-Call (chunked for long audio)
        try:
            total_dur = _wav_duration_sec(wav)
            print("WAV duration:", total_dur, "sec")

            start = 0.0
            idx = 0
            all_words = []

            while start < total_dur:
                chunk_dur = min(CHUNK_SEC, total_dur - start)
                chunk_path = os.path.join(td, f"chunk_{idx:04d}.wav")
                _cut_wav_chunk(wav, chunk_path, start, chunk_dur)

                print(f"STT chunk {idx} start={start:.2f}s dur={chunk_dur:.2f}s")
                chunk_words = _transcribe_wav_sync_words(client, recognizer, cfg, chunk_path, start)
                all_words.extend(chunk_words)

                start += chunk_dur
                idx += 1

            # Optional: transcript for fallback/debug
            text_out = " ".join(w["word"] for w in all_words).strip()

        except Exception as e:
            _fail_job(job, f"stt failed (chunked): {repr(e)} | recognizer={recognizer} lang={lang} model={model}")
            return

        # 5) Build REAL SRT from word timestamps
        srt_path = os.path.join(td, "out.srt")

        if all_words:
            print("Building segmented SRT from", len(all_words), "words")
            cues = segment_words_to_cues(all_words)
            srt_text = cues_to_srt(cues)
        else:
            print("WARNING: no word timestamps, falling back to single block")
            srt_text = (
                "1\n"
                f"00:00:00,000 --> {srt_timestamp(total_dur or 0.5)}\n"
                f"{text_out or '(хоосон стенограм)'}\n"
            )

        with open(srt_path, "w", encoding="utf-8") as f:
            f.write(srt_text)

        # Upload + DB Update
        base, _ = os.path.splitext(key)
        out_key = f"{base}.srt"
        try:
            if STORAGE_PROVIDER == "gcs":
                bucket = gcs_client.bucket(GCS_BUCKET)
                bucket.blob(out_key).upload_from_filename(
                    srt_path, content_type="application/x-subrip"
                )
                print("SRT uploaded (gcs):", out_key)
            else:
                s3.upload_file(srt_path, BUCKET, out_key)
                print("SRT uploaded (s3):", out_key)
        except Exception as e:
            _fail_job(job, f"upload srt failed: {repr(e)}")
            return

        job_id = job["job_id"]
        with db_engine.begin() as c:
            c.execute(text("""
              UPDATE jobs
              SET status='done',
                  srt_key=:s,
                  duration_sec=:d,
                  finished_at=now()
              WHERE id=:i
            """), {"s": out_key, "d": float(total_dur or 0.0), "i": job_id})
        print("JOB DONE:", job_id)


def main():
    print("WORKER STARTED")
    print("Redis:", REDIS_URL)
    print("Queue key:", QUEUE_KEY)
    print("Storage provider:", STORAGE_PROVIDER)

    try:
        print("REDIS PING:", r.ping())
    except Exception as e:
        print("REDIS PING FAILED:", e)
        raise

    if STORAGE_PROVIDER == "gcs":
        print(f"GCS mode enabled. Bucket: {GCS_BUCKET}")
        if not GCS_BUCKET:
            raise RuntimeError("GCS_BUCKET missing")
    else:
        try:
            s3.head_bucket(Bucket=BUCKET)
            print(f"S3 OK: bucket '{BUCKET}' erreichbar über {S3_ENDPOINT}")
        except Exception as e:
            print("S3 ERROR:", e)
            raise

    # ✅ Cloud Run Job pattern: get ONE job, process, exit
    print("Waiting for ONE job...")
    item = r.brpop(QUEUE_KEY, timeout=30)

    if not item:
        print("No job found within timeout, exiting.")
        return

    _, payload = item
    print("Got payload:", payload[:200], "..." if len(payload) > 200 else "")

    try:
        job = json.loads(payload)
    except Exception as e:
        print("Invalid JSON payload:", e)
        return

    # optional: immediately mark as processing (recommended)
    job_id = job.get("job_id")
    if job_id:
        try:
            with db_engine.begin() as c:
                c.execute(text("""
                  UPDATE jobs
                  SET status='processing'
                  WHERE id=:i
                """), {"i": job_id})
            print("JOB marked processing:", job_id)
        except Exception as e:
            print("DB update processing failed:", e)

    # process and exit
    process(job)
    print("Worker finished one job. Exiting.")


if __name__ == "__main__":
    main()
