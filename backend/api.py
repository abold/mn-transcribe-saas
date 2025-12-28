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
import stripe
import requests
import jwt
from jwt import PyJWKClient


from fastapi import FastAPI, HTTPException, Request
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

# ✅ SAFE Redis init (never crash the whole API at import time)
r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL)
    except Exception as e:
        print("⚠️ REDIS init failed:", repr(e))
        r = None
else:
    if os.getenv("K_SERVICE"):
        print("⚠️ REDIS_URL missing in Cloud Run environment (API will run but queue endpoints will fail)")


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
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://minio:9000")             # internal
PUBLIC_S3     = os.getenv("PUBLIC_S3_ENDPOINT", "http://localhost:9000")  # browser-visible

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET        = os.getenv("S3_BUCKET", "uploads")

# Storage switch
STORAGE_PROVIDER = os.getenv("STORAGE_PROVIDER", "minio").lower()  # "minio" | "gcs"
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
DEFAULT_SA_EMAIL = os.getenv("GCS_SIGNER_EMAIL", "")

MAX_DURATION_SEC = float(os.getenv("MAX_DURATION_SEC", "60"))

# Stripe settings
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

CREATOR_PRICE_ID = os.getenv("STRIPE_CREATOR_PRICE_ID")
PRO_PRICE_ID = os.getenv("STRIPE_PRO_PRICE_ID")
APP_BASE_URL = os.getenv("APP_BASE_URL")


def _require_stripe_config():
    missing = []
    if not stripe.api_key:
        missing.append("STRIPE_SECRET_KEY")
    if not CREATOR_PRICE_ID:
        missing.append("STRIPE_CREATOR_PRICE_ID")
    if not PRO_PRICE_ID:
        missing.append("STRIPE_PRO_PRICE_ID")
    if not APP_BASE_URL:
        missing.append("APP_BASE_URL")
    if missing:
        raise HTTPException(status_code=500, detail=f"Stripe config missing: {', '.join(missing)}")


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
# DB
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


def _require_redis():
    if r is None:
        raise HTTPException(status_code=500, detail="Redis not configured on server")
# ───────────────────────────────
# Supabase Auth + Billing DB helpers
# ───────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_JWT_AUD = os.getenv("SUPABASE_JWT_AUD", "authenticated").strip()
SUPABASE_JWT_ISSUER = os.getenv("SUPABASE_JWT_ISSUER", "").strip()

_jwks_client = None

def _require_supabase():
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_ROLE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise HTTPException(status_code=500, detail=f"Supabase config missing: {', '.join(missing)}")

def supabase_admin():
    # imported lazily so local env issues don't break import
    _require_supabase()
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def _get_jwks_client():
    global _jwks_client
    if _jwks_client is None:
        if not SUPABASE_URL:
            raise HTTPException(status_code=500, detail="SUPABASE_URL missing")
        jwks_url = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"
        _jwks_client = PyJWKClient(jwks_url)
    return _jwks_client

def require_user(request: Request) -> dict:
    """
    Verifies Supabase JWT from Authorization: Bearer <token>
    Returns: {"id": <uuid>, "email": <email or None>}
    """
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    if not auth or not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization Bearer token")

    token = auth.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Empty token")

    try:
        jwks_client = _get_jwks_client()
        signing_key = jwks_client.get_signing_key_from_jwt(token).key

        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=SUPABASE_JWT_AUD,
            options={"verify_exp": True},
            issuer=SUPABASE_JWT_ISSUER or None,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {type(e).__name__}")

    user_id = claims.get("sub")
    email = claims.get("email")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token missing sub")

    return {"id": user_id, "email": email}

def get_plan_limits_for_user(user_id: str):
    """
    Reads subscriptions + plan_limits + usage_monthly from Supabase DB.
    Requires the SQL tables we discussed:
      subscriptions(user_id, plan, status, ...)
      plan_limits(plan, monthly_seconds, max_file_seconds)
      usage_monthly(user_id, month, seconds_used)
    """
    sb = supabase_admin()

    # subscription row (defaults to free if missing/inactive)
    sub_res = sb.table("subscriptions").select("plan,status").eq("user_id", user_id).limit(1).execute()
    plan = "free"
    status = "inactive"
    if sub_res.data:
        plan = (sub_res.data[0].get("plan") or "free").lower()
        status = (sub_res.data[0].get("status") or "inactive").lower()
    if status not in ("active", "trialing"):
        plan = "free"

    lim_res = sb.table("plan_limits").select("monthly_seconds,max_file_seconds").eq("plan", plan).limit(1).execute()
    if lim_res.data:
        monthly_seconds = int(lim_res.data[0]["monthly_seconds"])
        max_file_seconds = int(lim_res.data[0]["max_file_seconds"])
    else:
        # fallback
        monthly_seconds = 10 * 60
        max_file_seconds = 60

    # month key = first day of current month (UTC)
    month = datetime.datetime.now(datetime.timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0).date().isoformat()
    usage_res = sb.table("usage_monthly").select("seconds_used").eq("user_id", user_id).eq("month", month).limit(1).execute()
    used_seconds = int(usage_res.data[0]["seconds_used"]) if usage_res.data else 0

    return plan, status, monthly_seconds, max_file_seconds, used_seconds, month

def reserve_usage_or_402(user_id: str, duration_sec: int):
    """
    Calls SQL RPC reserve_usage() for atomic reservation.
    If you haven't created it yet, do that (I gave you SQL earlier).
    """
    sb = supabase_admin()
    res = sb.rpc("reserve_usage", {"p_user_id": user_id, "p_duration_sec": int(duration_sec)}).execute()
    if not res.data or not res.data[0].get("ok"):
        raise HTTPException(status_code=402, detail="Monthly transcription limit reached")


# ───────────────────────────────
# Routes
# ───────────────────────────────

@app.get("/v1/version")
def version():
    return {
        "storage_provider": STORAGE_PROVIDER,
        "queue_key": QUEUE_KEY,
        "has_redis": r is not None,
        "max_duration_sec": MAX_DURATION_SEC,
        "git_sha": os.getenv("GIT_SHA", "unknown"),
        "image": os.getenv("K_REVISION", "unknown"),
    }


@app.get("/v1/redis-ping")
def redis_ping():
    _require_redis()
    return {"redis": bool(r.ping())}


@app.get("/v1/debug/queue-len")
def queue_len():
    _require_redis()
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
def create_job(j: CreateJobIn, request: Request):
    _require_redis()

    # ✅ require logged-in user
    user = require_user(request)
    user_id = user["id"]

    fk = _normalize_file_key(j.file_key)

    try:
        duration = _get_duration_from_storage(fk)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read media duration: {e}")

    # ✅ PLAN-BASED LIMITS with safe fallback
    try:
        plan, status, monthly_sec, max_file_sec, used_sec, month = get_plan_limits_for_user(user_id)

        if duration > max_file_sec:
            raise HTTPException(
                status_code=402,
                detail=f"File too long for plan '{plan}': {duration/60:.1f} min (max {max_file_sec/60:.0f} min)"
            )

        # ✅ Reserve monthly usage atomically (prevents spam abuse)
        reserve_usage_or_402(user_id, int(duration))

    except Exception as e:
        # ✅ fallback until Supabase billing tables / RPC exist
        logger.warning(
            "Plan/usage lookup failed; falling back to MAX_DURATION_SEC. err=%r",
            e,
        )

        if duration > MAX_DURATION_SEC:
            raise HTTPException(
                status_code=400,
                detail=f"Media too long: {duration:.1f}s (max {MAX_DURATION_SEC:.0f}s)"
            )

        plan = "free"
        month = None

    job_id = str(uuid.uuid4())

    with engine.begin() as c:
        c.execute(
            text("""
                INSERT INTO jobs(id,file_key,status,engine,duration_sec)
                VALUES(:i,:k,'queued',:e,:d)
            """),
            {"i": job_id, "k": fk, "e": j.engine, "d": duration},
        )

    payload = {
        "job_id": job_id,
        "file_key": fk,
        "engine": j.engine,
        "user_id": user_id,   # for traceability
        "plan": plan,
        "month": month,
    }

    try:
        logger.info("enqueue: queue_key=%s redis_url=%s", QUEUE_KEY, mask_redis_url(REDIS_URL))
        pong = r.ping()
        logger.info("enqueue: redis_ping=%s", pong)

        new_len = r.lpush(QUEUE_KEY, json.dumps(payload))
        logger.info("enqueue: lpush_ok new_len=%s job_id=%s", new_len, job_id)

    except Exception as e:
        logger.exception("enqueue: FAILED error=%r", e)
        raise HTTPException(
            status_code=500,
            detail=f"Redis enqueue failed: {type(e).__name__}: {e}",
        )

    return {
        "id": job_id,
        "status": "queued",
        "queue_key": QUEUE_KEY,
        "queue_len_after": new_len,
    }



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

class LimitsCheckIn(BaseModel):
    duration_sec: int

@app.post("/v1/limits/check")
def limits_check(body: LimitsCheckIn, request: Request):
    user = require_user(request)

    plan, status, monthly_sec, max_file_sec, used_sec, _month = get_plan_limits_for_user(user["id"])

    if body.duration_sec > max_file_sec:
        raise HTTPException(status_code=402, detail=f"File too long for plan '{plan}' (max {max_file_sec//60} min)")

    if used_sec + body.duration_sec > monthly_sec:
        raise HTTPException(status_code=402, detail=f"Monthly limit reached for plan '{plan}'")

    return {"ok": True, "plan": plan, "used_sec": used_sec, "monthly_sec": monthly_sec, "max_file_sec": max_file_sec}


# ───────────────────────────────
# Stripe Billing
# ───────────────────────────────

class CheckoutIn(BaseModel):
    plan: str

@app.post("/v1/billing/checkout")
def create_checkout_session(body: CheckoutIn):
    _require_stripe_config()

    plan = body.plan
    if plan == "creator":
        price_id = CREATOR_PRICE_ID
    elif plan == "pro":
        price_id = PRO_PRICE_ID
    else:
        raise HTTPException(status_code=400, detail="Invalid plan")

    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=f"{APP_BASE_URL}/billing/success",
        cancel_url=f"{APP_BASE_URL}/billing/cancel",
    )
    return {"url": session.url}


@app.post("/v1/billing/webhook")
async def stripe_webhook(request: Request):
    webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET")
    sig = request.headers.get("stripe-signature")
    if not webhook_secret:
        raise HTTPException(status_code=500, detail="STRIPE_WEBHOOK_SECRET missing")
    if not sig:
        raise HTTPException(status_code=400, detail="Missing stripe-signature header")

    payload = await request.body()

    try:
        event = stripe.Webhook.construct_event(payload, sig, webhook_secret)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload")

    # ✅ Now safe to use event
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        # TODO: save subscription

    elif event["type"] == "customer.subscription.deleted":
        sub = event["data"]["object"]
        # TODO: downgrade

    return {"ok": True}


