import os
import uuid
import json
import datetime
import subprocess
import tempfile
import re
import logging
import google.auth
import google.auth.transport.requests
from google.cloud import storage

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

import boto3
import redis
from botocore.client import Config
from botocore.exceptions import ClientError


# ───────────────────────────────
# ENV laden (NUR lokal!)
# Cloud Run setzt K_SERVICE → dann NICHT .env laden
# ───────────────────────────────
if not os.getenv("K_SERVICE"):
    from dotenv import load_dotenv
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"))


DB_URL = os.getenv("DATABASE_URL", "sqlite:///./app.db")

REDIS_URL = os.getenv("REDIS_URL")
if os.getenv("K_SERVICE") and not REDIS_URL:
    raise RuntimeError("REDIS_URL missing in Cloud Run environment")

r = redis.from_url(REDIS_URL)

# ───────── Logging ─────────
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("mn-transcribe")

QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY", "mn:q")

def mask_redis_url(url: str) -> str:
    if not url:
        return ""
    if "@" in url and "://" in url:
        scheme, rest = url.split("://", 1)
        creds, host = rest.split("@", 1)
        return f"{scheme}://***@{host}"
    return "***"



# MinIO/S3 settings (nur relevant wenn STORAGE_PROVIDER=minio)
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://minio:9000")         # internal
PUBLIC_S3     = os.getenv("PUBLIC_S3_ENDPOINT", "http://localhost:9000")  # browser-visible

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET        = os.getenv("S3_BUCKET", "uploads")

# Storage switch
STORAGE_PROVIDER = os.getenv("STORAGE_PROVIDER", "minio").lower()  # "minio" | "gcs"
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
DEFAULT_SA_EMAIL = os.getenv("GCS_SIGNER_EMAIL", "")

MAX_DURATION_SEC = float(os.getenv("MAX_DURATION_SEC", "60"))


# ───────────────────────────────
# FastAPI & CORS
# ───────────────────────────────
app = FastAPI(title="MN Transcribe API")

# If you want strict origins, set ALLOWED_ORIGINS in Cloud Run:
# ALLOWED_ORIGINS="https://your-vercel-url.vercel.app,https://your-domain.com"
_allowed = os.getenv("ALLOWED_ORIGINS", "").strip()
if _allowed:
    allow_origins = [x.strip() for x in _allowed.split(",") if x.strip()]
else:
    allow_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,  # with "*" must be False
)


# ───────────────────────────────
# DB & Redis
# ───────────────────────────────
engine = create_engine(
    DB_URL,
    future=True,
    pool_pre_ping=True,   # checks connection before using it
    pool_recycle=300,     # recycle connections every 5 min
)


def is_postgres(url: str) -> bool:
    try:
        return make_url(url).get_backend_name().startswith("postgres")
    except Exception:
        return False

with engine.begin() as c:
    if is_postgres(DB_URL):
        c.execute(text("""
        CREATE TABLE IF NOT EXISTS jobs(
          id uuid primary key,
          file_key text not null,
          status text not null,
          engine text,
          srt_key text,
          duration_sec real,
          error_msg text,
          created_at timestamptz default now(),
          finished_at timestamptz
        )
        """))
    else:
        c.execute(text("""
        CREATE TABLE IF NOT EXISTS jobs(
          id text primary key,
          file_key text not null,
          status text not null,
          engine text,
          srt_key text,
          duration_sec real,
          error_msg text,
          created_at text,
          finished_at text
        )
        """))


# ───────────────────────────────
# Storage clients
# ───────────────────────────────
gcs_client = storage.Client() if STORAGE_PROVIDER == "gcs" else None

s3_internal = None
s3_presign = None

if STORAGE_PROVIDER == "minio":
    s3_internal = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    s3_presign = boto3.client(
        "s3",
        endpoint_url=PUBLIC_S3,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


# ───────────────────────────────
# Helpers
# ───────────────────────────────
def gcs_presign_put(file_key: str, content_type: str | None, expires_sec: int = 3600) -> str:
    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET missing")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(file_key)
    return blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(seconds=expires_sec),
        method="PUT",
        content_type=content_type or "application/octet-stream",
    )

def gcs_presign_get(file_key: str, expires_sec: int = 3600) -> str:
    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET missing")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(file_key)
    return blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(seconds=expires_sec),
        method="GET",
    )

def _to_obj_key(k: str | None) -> str | None:
    if not k:
        return None
    k = k.lstrip("/")
    if k.startswith(f"{BUCKET}/"):
        k = k[len(BUCKET) + 1:]
    return k

def _presign_get_or_none(key: str | None) -> str | None:
    obj_key = _to_obj_key(key)
    if not obj_key:
        return None

    if STORAGE_PROVIDER == "gcs":
        if not GCS_BUCKET or not gcs_client:
            return None
        bucket = gcs_client.bucket(GCS_BUCKET)
        blob = bucket.blob(obj_key)
        if not blob.exists():
            return None

        creds, _ = google.auth.default()
        req = google.auth.transport.requests.Request()
        creds.refresh(req)

        sa_email = DEFAULT_SA_EMAIL or getattr(creds, "service_account_email", None)

        return blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(hours=1),
            method="GET",
            service_account_email=sa_email,
            access_token=creds.token,
        )

    # MinIO
    try:
        assert s3_internal and s3_presign
        s3_internal.head_object(Bucket=BUCKET, Key=obj_key)
        return s3_presign.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET, "Key": obj_key},
            ExpiresIn=3600,
        )
    except Exception:
        return None

def _ffprobe_duration_seconds(path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or "ffprobe failed")
    out = (p.stdout or "").strip()
    if not out:
        raise RuntimeError("ffprobe returned empty duration")
    return float(out)

def _get_duration_from_storage(obj_key: str) -> float:
    suffix = ".bin"
    if obj_key and "." in obj_key:
        suffix = "." + obj_key.rsplit(".", 1)[-1]

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
        if STORAGE_PROVIDER == "gcs":
            if not GCS_BUCKET or not gcs_client:
                raise RuntimeError("GCS_BUCKET missing")
            bucket = gcs_client.bucket(GCS_BUCKET)
            blob = bucket.blob(obj_key)
            if not blob.exists():
                raise RuntimeError(f"object not found in GCS: {obj_key}")
            blob.download_to_filename(tmp.name)

        else:
            assert s3_internal
            try:
                obj = s3_internal.get_object(Bucket=BUCKET, Key=obj_key)
            except ClientError as e:
                raise RuntimeError(f"object not found in S3/MinIO: {obj_key}") from e

            body = obj["Body"]
            try:
                for chunk in iter(lambda: body.read(1024 * 1024), b""):
                    tmp.write(chunk)
                tmp.flush()
            finally:
                body.close()

        return _ffprobe_duration_seconds(tmp.name)

def _safe_filename_base(name: str) -> str:
    # keep only safe characters; convert everything else (incl spaces) to underscores
    s = (name or "").strip()
    s = re.sub(r"\s+", "_", s)                 # spaces -> _
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)    # other unsafe chars -> _
    s = re.sub(r"_+", "_", s).strip("._-")     # cleanup
    return s or "file"

# ───────────────────────────────
# Routes
# ───────────────────────────────
@app.get("/")
def root():
    return {"ok": True, "service": "mn-transcribe-api", "storage_provider": STORAGE_PROVIDER}

@app.get("/v1/redis-ping")
def redis_ping():
    return {"redis": bool(r.ping())}

@app.get("/v1/debug/queue-len")
def queue_len():
    return {"queue_key": QUEUE_KEY, "len": r.llen(QUEUE_KEY)}



class PresignIn(BaseModel):
    filename: str
    content_type: str | None = None

@app.post("/v1/presign")
def presign(p: PresignIn):
    base, ext = os.path.splitext(p.filename)
    ext = ext or ".bin"
    safe_base = _safe_filename_base(base)
    object_key = f"{uuid.uuid4()}_{safe_base}{ext}"


    if STORAGE_PROVIDER == "gcs":
        if not GCS_BUCKET or not gcs_client:
            raise HTTPException(status_code=500, detail="GCS_BUCKET missing")

        bucket = gcs_client.bucket(GCS_BUCKET)
        blob = bucket.blob(object_key)

        creds, _ = google.auth.default()
        req = google.auth.transport.requests.Request()
        creds.refresh(req)

        sa_email = DEFAULT_SA_EMAIL or getattr(creds, "service_account_email", None)

        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(hours=1),
            method="PUT",
            content_type=p.content_type or "application/octet-stream",
            service_account_email=sa_email,
            access_token=creds.token,
        )

        return {"key": object_key, "file_key": object_key, "url": url}

    # MinIO fallback
    if not s3_presign:
        raise HTTPException(status_code=500, detail="S3 client not configured")

    params = {"Bucket": BUCKET, "Key": object_key}
    if p.content_type:
        params["ContentType"] = p.content_type

    url = s3_presign.generate_presigned_url("put_object", Params=params, ExpiresIn=3600)
    return {"key": object_key, "file_key": object_key, "url": url}


class CreateJobIn(BaseModel):
    file_key: str
    engine: str = "google-stt-v2"

    @field_validator("file_key")
    @classmethod
    def non_empty(cls, v: str):
        v2 = (v or "").strip().lower()
        if not v2 or v2 == "null":
            raise ValueError("file_key invalid")
        return v

def _normalize_file_key(k: str) -> str:
    k = (k or "").lstrip("/")
    if k.startswith(f"{BUCKET}/"):
        k = k[len(BUCKET) + 1:]
    return k

@app.post("/v1/jobs")
def create_job(j: CreateJobIn):
    fk = _normalize_file_key(j.file_key)

    try:
        duration = _get_duration_from_storage(fk)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read media duration: {e}")

    if duration > MAX_DURATION_SEC:
        raise HTTPException(status_code=400, detail=f"Media too long: {duration:.1f}s (max {MAX_DURATION_SEC:.0f}s)")

    job_id = str(uuid.uuid4())

    with engine.begin() as c:
        c.execute(
            text("""
                INSERT INTO jobs(id,file_key,status,engine,duration_sec)
                VALUES(:i,:k,'queued',:e,:d)
            """),
            {"i": job_id, "k": fk, "e": j.engine, "d": duration},
        )

    payload = {"job_id": job_id, "file_key": fk, "engine": j.engine}

    try:
        logger.info("enqueue: queue_key=%s redis_url=%s", QUEUE_KEY, mask_redis_url(REDIS_URL))
        pong = r.ping()
        logger.info("enqueue: redis_ping=%s", pong)

        new_len = r.lpush(QUEUE_KEY, json.dumps(payload))
        logger.info("enqueue: lpush_ok new_len=%s job_id=%s", new_len, job_id)

    except Exception as e:
        logger.exception("enqueue: FAILED error=%r", e)
        raise HTTPException(status_code=500, detail=f"Redis enqueue failed: {type(e).__name__}: {e}")

    return {"id": job_id, "status": "queued"}


@app.get("/v1/jobs/{job_id}")
def get_job(job_id: str):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, status, srt_key, file_key, duration_sec, error_msg FROM jobs WHERE id = :i"),
            {"i": job_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="not found")

    srt_url  = _presign_get_or_none(row["srt_key"])
    file_url = _presign_get_or_none(row["file_key"])

    return {
        "id": row["id"],
        "status": row["status"],
        "file_key": row["file_key"],
        "duration_sec": row["duration_sec"],
        "srt_key": row["srt_key"],
        "error_msg": row.get("error_msg"),
        "file_url": file_url,
        "srt_url": srt_url,
    }
