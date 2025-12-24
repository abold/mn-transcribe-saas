# stdlib
import os
import tempfile
import datetime as dt
import glob, wave

# third-party
import ffmpeg
import srt

# Google STT v2
from google.api_core.client_options import ClientOptions
from google.cloud import speech_v2 as speech
from google.cloud.speech_v2.types import cloud_speech

# ------------------------------------------------------------------------------------
# Google STT v2 (mn-MN)
# ------------------------------------------------------------------------------------
def google_stt_v2(wav_path: str, region: str = "europe-west4", vocab_hint: str = "") -> dict:
    """
    Returns:
      {"text": <full text>, "words": [(word, start_sec, end_sec), ...]}
    Uses a regional endpoint that must match the recognizer location.
    Set region to "global" if you prefer the global endpoint.
    """
    api_endpoint = "speech.googleapis.com" if region == "global" else f"{region}-speech.googleapis.com"
    client = speech.SpeechClient(client_options=ClientOptions(api_endpoint=api_endpoint))

    # Get project id from env or gcloud config
    project_id = os.environ.get("GCP_PROJECT_ID") or getattr(client, "project", None)
    if not project_id:
        raise RuntimeError("Set GCP_PROJECT_ID or run `gcloud config set project <id>`.")

    recognizer = f"projects/{project_id}/locations/{region}/recognizers/_"

    with open(wav_path, "rb") as f:
        audio_content = f.read()

    # Phrase hints (Speech Adaptation)
    phrases = [p.strip() for p in (vocab_hint or "").split(",") if p.strip()]
    adaptation = None
    if phrases:
        adaptation = cloud_speech.SpeechAdaptation(
            phrase_sets=[
                cloud_speech.SpeechAdaptation.AdaptationPhraseSet(
                    inline_phrase_set=cloud_speech.PhraseSet(
                        phrases=[cloud_speech.PhraseSet.Phrase(value=p) for p in phrases],
                        boost=20.0
                    )
                )
            ]
        )

    config = cloud_speech.RecognitionConfig(
        auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
        language_codes=["mn-MN"],
        model="chirp_2",
        features=cloud_speech.RecognitionFeatures(
            enable_automatic_punctuation=True,
            enable_word_time_offsets=True,
        ),
        adaptation=adaptation,
    )

    request = cloud_speech.RecognizeRequest(
        recognizer=recognizer,
        config=config,
        content=audio_content,
    )
    resp = client.recognize(request=request)

    lines, words = [], []
    for r in resp.results:
        if not r.alternatives:
            continue
        alt = r.alternatives[0]
        if alt.transcript:
            lines.append(alt.transcript.strip())
        for w in alt.words:
            words.append((w.word, w.start_offset.total_seconds(), w.end_offset.total_seconds()))

    return {"text": "\n".join(lines).strip(), "words": words}

# ------------------------------------------------------------------------------------
# Helpers: SRT, Chunking, Audio
# ------------------------------------------------------------------------------------
SAMPLE_RATE = 16000

def extract_audio_to_wav(input_path: str, out_dir: str) -> str:
    out_wav = os.path.join(out_dir, "audio_16k.wav")
    (
        ffmpeg
        .input(input_path)
        .output(out_wav, ac=1, ar=SAMPLE_RATE, format="wav", acodec="pcm_s16le")
        .overwrite_output()
        .run(quiet=True)
    )
    if not os.path.exists(out_wav):
        raise RuntimeError("Failed to create WAV. Is ffmpeg installed?")
    return out_wav

def get_wav_duration_sec(wav_path: str) -> float:
    with wave.open(wav_path, "rb") as wf:
        return wf.getnframes() / float(wf.getframerate())

def split_wav_chunks(wav_path: str, chunk_sec: float = 55.0) -> list[str]:
    """
    Split a 16k mono PCM WAV into ~chunk_sec pieces using ffmpeg segmenter.
    Returns a sorted list of chunk file paths in a temp dir.
    """
    tmpdir = tempfile.mkdtemp(prefix="mn_chunks_")
    out_pattern = os.path.join(tmpdir, "chunk_%04d.wav")
    (
        ffmpeg
        .input(wav_path)
        .output(
            out_pattern,
            f="segment",
            segment_time=chunk_sec,
            c="copy",                # fast, no re-encode (already PCM 16k)
            reset_timestamps=1
        )
        .overwrite_output()
        .run(quiet=True)
    )
    files = sorted(glob.glob(os.path.join(tmpdir, "chunk_*.wav")))
    if not files:
        raise RuntimeError("Chunking failed; no segments produced.")
    return files

def google_stt_v2_long(wav_path: str, region: str, vocab_hint: str, chunk_sec: float = 55.0) -> dict:
    """
    Calls google_stt_v2 on consecutive ~chunk_sec chunks and stitches word timestamps.
    Returns {"text": "<full transcript>", "words": [...]}
    """
    chunks = split_wav_chunks(wav_path, chunk_sec=chunk_sec)
    all_words = []
    base_offset = 0.0
    texts = []
    for ch in chunks:
        res = google_stt_v2(ch, region=region, vocab_hint=vocab_hint)
        if res.get("text"):
            texts.append(res["text"])
        for (w, s, e) in res.get("words", []):
            all_words.append((w, s + base_offset, e + base_offset))
        with wave.open(ch, "rb") as wf:
            base_offset += wf.getnframes() / float(wf.getframerate())
    full_text = "\n".join(t for t in texts if t).strip()
    return {"text": full_text, "words": all_words}

def filter_garbage_words(words):
    clean = []
    for (w, s, e) in words:
        if not w or not w.strip():
            continue
        t = w.strip()
        if t.isdigit():           # "0", "1", "2025"
            continue
        if set(t) == {"0"}:       # "0000"
            continue
        clean.append((t, s, e))
    return clean

def looks_like_zero_gibberish(words, sample_n=60):
    sample = [w for (w, _, _) in words[:sample_n]]
    if not sample:
        return False
    zeros = sum(1 for t in sample if (t.isdigit() or set(t) == {"0"}))
    return zeros / len(sample) > 0.6

def words_to_srt(words, max_words=12, min_dur=0.6) -> str:
    """Chunk words into subtitle lines with timestamps."""
    if not words:
        return ""
    subs = []
    i = 0
    while i < len(words):
        j = min(i + max_words, len(words))
        start = words[i][1]
        end   = max(words[j-1][2], start + min_dur)
        content = " ".join(w for (w, _, _) in words[i:j]).strip()
        subs.append(srt.Subtitle(
            index=len(subs)+1,
            start=dt.timedelta(seconds=start),
            end=dt.timedelta(seconds=end),
            content=content
        ))
        i = j
    return srt.compose(subs)

def text_to_srt_by_blocks(text: str, start_sec: float = 0.0,
                          words_per_line: int = 12, dur_per_line: float = 6.0) -> str:
    tokens = text.split()
    subs, t, idx = [], start_sec, 1
    for i in range(0, len(tokens), words_per_line):
        content = " ".join(tokens[i:i+words_per_line]).strip()
        subs.append(srt.Subtitle(
            index=idx,
            start=dt.timedelta(seconds=t),
            end=dt.timedelta(seconds=t + dur_per_line),
            content=content or "…"
        ))
        t += dur_per_line
        idx += 1
    return srt.compose(subs)

# ------------------------------------------------------------------------------------
# Public API für den Worker: gibt SRT-String + Meta zurück (schreibt KEINE Datei)
# ------------------------------------------------------------------------------------
def transcribe_to_srt_string(input_path: str) -> tuple[str, dict]:
    """
    High-level: Nimmt ein Video/Audio-File, extrahiert Audio, ruft Google STT auf
    (short vs. long), baut SRT und gibt (srt_text, meta) zurück.
    """
    if not input_path or not os.path.exists(input_path):
        raise RuntimeError("No file received. Please provide a valid video/audio path.")

    # 1) Extract audio
    workdir = tempfile.mkdtemp(prefix="mn_subs_")
    wav_path = extract_audio_to_wav(input_path, workdir)

    # 2) Transcribe via Google STT v2 (add your own hints)
    hints = "Улаанбаатар, Монгол Улс, хиймэл оюун ухаан, судалгаа, оюутан, шалгалт, семестр"

    dur = get_wav_duration_sec(wav_path)
    if dur <= 59.5:
        result = google_stt_v2(wav_path, region=os.getenv("GCP_REGION", "europe-west4"), vocab_hint=hints)
    else:
        result = google_stt_v2_long(wav_path, region=os.getenv("GCP_REGION", "europe-west4"), vocab_hint=hints, chunk_sec=58.0)

    if not result.get("words"):
        # Fallback: aus Volltext Blöcke bauen
        srt_str = text_to_srt_by_blocks(result.get("text", ""), start_sec=0.0, words_per_line=12, dur_per_line=6.0)
    else:
        words = filter_garbage_words(result["words"])
        if looks_like_zero_gibberish(words):
            srt_str = text_to_srt_by_blocks(result.get("text", ""), start_sec=0.0, words_per_line=12, dur_per_line=6.0)
        else:
            srt_str = words_to_srt(words)

    meta = {
        "engine": "google-stt-v2",
        "sample_rate": SAMPLE_RATE,
        "duration_sec": dur,
        "region": os.getenv("GCP_REGION", "europe-west4")
    }
    return srt_str, meta


