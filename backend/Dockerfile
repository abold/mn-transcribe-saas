FROM python:3.11-slim

# Systempakete (ffmpeg für Audio-Extraktion; ca-certificates schadet nie)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies zuerst kopieren für besseren Docker-Cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip uninstall -y google || true
RUN python -c "from google.cloud import storage; print('google-cloud-storage OK:', storage.__version__)"

# App-Code
COPY . .
EXPOSE 8080

# Standard-CMD für den API-Container (der worker überschreibt das via docker-compose 'command')
CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT:-8080}"]

