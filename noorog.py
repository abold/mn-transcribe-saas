gcloud run services update mn-transcribe-api \
  --region europe-west1 \
  --set-env-vars SUPABASE_JWT_SECRET="gyMxzF6RpkGTQXgrQUpF1bbT5tSOioP3XL59cu+eCbo6dv41uuOw+ltNnA/zbP05CARP/X24rKa+WJQvd/QSJw==",SUPABASE_URL="https://aiwlapshwyqkrxwqtlsf.supabase.co"


gcloud run services update mn-transcribe-api \
  --region europe-west1 \
  --set-env-vars REDIS_URL="rediss://default:ATWcAAIncDE4NzBlOTBiMmIxMzM0N2IyYTA3M2NmNzZmNWE1MzczNnAxMTM3MjQ@caring-wahoo-13724.upstash.io:6379"
gcloud run jobs update mn-transcribe-worker \
  --region europe-west1 \
  --set-env-vars REDIS_URL="rediss://default:ATWcAAIncDE4NzBlOTBiMmIxMzM0N2IyYTA3M2NmNzZmNWE1MzczNnAxMTM3MjQ@caring-wahoo-13724.upstash.io:6379"

gcloud run services update mn-transcribe-api \
  --region=europe-west1 \
  --set-env-vars REDIS_URL="rediss://default:ATWcAAIncDE4NzBlOTBiMmIxMzM0N2IyYTA3M2NmNzZmNWE1MzczNnAxMTM3MjQ@caring-wahoo-13724.upstash.io:6379"

gcloud run services update mn-transcribe-api \
  --region=europe-west1 \
  --set-env-vars \
REDIS_URL="rediss://default:ATWcAAIncDE4NzBlOTBiMmIxMzM0N2IyYTA3M2NmNzZmNWE1MzczNnAxMTM3MjQ@caring-wahoo-13724.upstash.io:6379",REDIS_QUEUE_KEY="mn:q"


gcloud run services update mn-transcribe-api \
  --region=europe-west1 \
  --set-env-vars REDIS_URL="rediss://default:<TOKEN>@caring-wahoo-13724.upstash.io:6379",REDIS_QUEUE_KEY="mn:q"


API="https://mn-transcribe-api-1076791344893.europe-west1.run.app"

# 1) Get presign (stores JSON into RESP)
RESP=$(curl -s -X POST "$API/v1/presign" \
  -H "Content-Type: application/json" \
  -d '{"filename":"test.mov","content_type":"video/quicktime"}')

echo "PRESIGN RESP:"
echo "$RESP"

# 2) Extract URL + FILE_KEY from RESP (pass via stdin, not env)
URL=$(printf "%s" "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["url"])')
FILE_KEY=$(printf "%s" "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["file_key"])')

echo "FILE_KEY=$FILE_KEY"
echo "URL=$URL"

# 3) Pick a real local file (CHANGE THIS PATH)
FILEPATH="/Users/anar/Downloads/AAAAAAAA.mov"
ls -lh "$FILEPATH" || exit 1

# 4) Upload to GCS using the signed URL (Content-Type must match presign)
echo "Uploading..."
curl -i -X PUT "$URL" \
  -H "Content-Type: video/quicktime" \
  --upload-file "$FILEPATH"

# 5) Confirm object exists in bucket
echo "Checking GCS object..."
gsutil ls -l "gs://mn-transcribe-uploads-anar/$FILE_KEY"

# 6) Create job (this is where we’ll see the REAL reason for HTTP 400 if it happens)
echo "Creating job..."
curl -i -X POST "$API/v1/jobs" \
  -H "Content-Type: application/json" \
  -d "{\"file_key\":\"$FILE_KEY\",\"engine\":\"google-stt-v2\"}"

# 7) Check queue length
echo
echo "Queue length:"
curl -s "$API/v1/debug/queue-len"
echo
