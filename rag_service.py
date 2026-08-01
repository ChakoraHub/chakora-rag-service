"""
rag_service.py  ─  Multimodal RAG Ingestion & Retrieval Pipeline
=================================================================
Framework     : LlamaIndex (llama-index-core ≥ 0.10)
Architecture  : Multimodal RAG + RAG with memory
Chunking      : Semantic Chunking  (LlamaIndex SemanticSplitterNodeParser)
Embedding     : BGE-M3  (text)  +  nomic-embed-vision-v1.5  (images)
Retrieval     : Hybrid Search (dense + BM25 sparse) + BAAI/bge-reranker-v2-m3
LLM           : meta-llama/Llama-3-8B-Instruct via Ollama
Vector Store  : Redis  (port 6379)   DB 9 = dense vectors   DB 10 = BM25   (DB 0–8 reserved for cache/chatbot/meeting)
S3 Source     : s3://chakorahub-rag-s3/ChakoraHub-Org-Docs/
Port          : 7900

Install:
  pip install "llama-index-core>=0.10" llama-index-embeddings-huggingface
  pip install llama-index-vector-stores-redis llama-index-postprocessor-flag-embedding-reranker
  pip install llama-index-readers-s3 llama-index-readers-file
  pip install FlagEmbedding sentence-transformers transformers torch
  pip install fastapi uvicorn boto3 redis pillow pypdf python-docx
  pip install rank-bm25 httpx numpy python-dotenv

Run:
  uvicorn rag_service:app --host 0.0.0.0 --port 7900 --workers 1
"""

from __future__ import annotations

import base64
from curses import raw
from curses import raw
import hashlib
import io
import json
import math
import os
import re
import struct
import threading
import tempfile
import traceback
import urllib.parse
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import boto3
import httpx
import numpy as np
import redis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from llama_index.core import SimpleDirectoryReader
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.core.settings import Settings
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
#from llama_index.llms.huggingface import HuggingFaceLLM
from llama_index.llms.ollama import Ollama
from llama_index.postprocessor.flag_embedding_reranker import (
    FlagEmbeddingReranker
)
from llama_index.core.memory import ChatMemoryBuffer
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

S3_BUCKET  = os.getenv("S3_BUCKET_NAME", "chakorahub-rag-s3")
S3_PREFIX  = os.getenv("S3_PREFIX",      "ChakoraHub-Org-Docs/")
AWS_REGION = os.getenv("AWS_REGION",     "eu-north-1")
TRANSCRIPT_EVENT_BUCKET = os.getenv("TRANSCRIPT_BUCKET", "chakorahub-meeting-s3")
TRANSCRIPT_EVENT_PREFIX = os.getenv("TRANSCRIPT_PREFIX", "transcripts/")

REDIS_HOST     = os.getenv("REDIS_HOST",         "localhost")
REDIS_RAG_PORT = int(os.getenv("REDIS_RAG_PORT", "6379"))
REDIS_RAG_PASS = os.getenv("REDIS_RAG_PASSWORD", None)
REDIS_VEC_DB   = int(os.getenv("REDIS_VECTOR_DB", "9"))   # dense vectors + metadata  (DB 0–8 reserved for redis_service)
REDIS_BM25_DB  = int(os.getenv("REDIS_BM25_DB",   "10"))  # BM25 inverted index

TEXT_EMBED_MODEL  = os.getenv("TEXT_EMBED_MODEL",  "BAAI/bge-m3")
IMAGE_EMBED_MODEL = os.getenv("IMAGE_EMBED_MODEL", "nomic-ai/nomic-embed-vision-v1.5")
RERANKER_MODEL    = os.getenv("RERANKER_MODEL",    "BAAI/bge-reranker-v2-m3")

OLLAMA_API  = os.getenv("OLLAMA_API",   "http://127.0.0.1:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")   # ollama tag for Llama-3-8B-Instruct

TEXT_EMBED_DIM  = 1024   # BGE-M3
IMAGE_EMBED_DIM = 768    # nomic-embed-vision-v1.5

# Semantic chunking (LlamaIndex SemanticSplitterNodeParser)
SEMANTIC_THRESHOLD  = float(os.getenv("SEMANTIC_THRESHOLD", "0.75"))
MAX_CHUNK_WORDS     = int(os.getenv("MAX_CHUNK_WORDS",      "300"))
MIN_CHUNK_WORDS     = int(os.getenv("MIN_CHUNK_WORDS",      "30"))
SENTENCE_WINDOW     = int(os.getenv("SENTENCE_WINDOW",      "3"))

# Hybrid retrieval
HYBRID_DENSE_TOP_K = int(os.getenv("HYBRID_DENSE_TOP_K", "20"))
HYBRID_BM25_TOP_K  = int(os.getenv("HYBRID_BM25_TOP_K",  "20"))
HYBRID_ALPHA       = float(os.getenv("HYBRID_ALPHA",      "0.6"))   # 1=dense-only 0=BM25-only
RERANK_TOP_K       = int(os.getenv("RERANK_TOP_K",        "5"))

AUTO_INGEST_ON_STARTUP = os.getenv("AUTO_INGEST_ON_STARTUP", "false").lower() == "true"
AUTO_INGEST_FORCE      = os.getenv("AUTO_INGEST_FORCE",      "false").lower() == "true"
ENABLE_KAFKA_PIPELINE  = os.getenv("ENABLE_KAFKA_PIPELINE",  "true").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEBUG_MEETING_COMPLETED_BOOKING_ID = os.getenv(
    "DEBUG_MEETING_COMPLETED_BOOKING_ID",
    "275a3ee2-5e19-4205-b784-414e06c32c18",
).strip()

# Redis key namespaces
CHUNK_PFX      = "rag:chunk:"
BM25_TERM_PFX  = "rag:bm25:term:"
BM25_META_KEY  = "rag:bm25:meta"
DOC_IDX_KEY    = "rag:doc:index"
ETAG_KEY       = "rag:doc:etags"

SUPPORTED_TEXT_EXT  = {".txt", ".md", ".csv", ".json", ".html", ".htm",
                        ".py", ".yaml", ".yml"}
SUPPORTED_DOCX_EXT  = {".docx"}
SUPPORTED_PDF_EXT   = {".pdf"}
SUPPORTED_IMAGE_EXT = {".png", ".jpg", ".jpeg", ".webp",
                        ".gif", ".bmp", ".tiff"}

# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="Multimodal RAG Service (LlamaIndex)",
    description="LlamaIndex-powered ingestion + retrieval for ChakoraHub Org Docs",
    version="3.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════
# REDIS CLIENTS
# ═══════════════════════════════════════════════════════════════

def _make_redis(db: int, decode: bool = False) -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_RAG_PORT,
        db=db, password=REDIS_RAG_PASS,
        decode_responses=decode,
    )

redis_vec      = _make_redis(REDIS_VEC_DB,  decode=False)   # binary, for vector bytes
redis_bm25_str = _make_redis(REDIS_BM25_DB, decode=True)    # text, for BM25 index

for _c, _l in [(redis_vec, "Vec-DB"), (redis_bm25_str, "BM25-DB")]:
    try:
        _c.ping()
        print(f"✅ Redis {_l} connected  ({REDIS_HOST}:{REDIS_RAG_PORT})")
    except Exception as _e:
        print(f"❌ Redis {_l} connection failed: {_e}")

# ═══════════════════════════════════════════════════════════════
# KAFKA HELPERS
# ═══════════════════════════════════════════════════════════════

_kafka_producer = None
try:
    _kafka_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )
    print("✅ Kafka producer connected (rag_service)")
except Exception as _kafka_err:
    print(f"⚠️  Kafka producer unavailable (rag_service): {_kafka_err}")


def _kafka_publish(topic: str, payload: Dict[str, Any]) -> None:
    if _kafka_producer is None:
        print(f"⚠️  Kafka publish skipped [{topic}] because producer is unavailable")
        return
    try:
        print(f"📤 Kafka publish request → {topic} | keys={list(payload.keys())}")
        _kafka_producer.send(topic, value=payload)
        _kafka_producer.flush(timeout=2)
        print(f"📤 Kafka → {topic}: {payload}")
    except Exception as e:
        print(f"⚠️  Kafka publish failed [{topic}]: {e}")


def _resolve_s3_reference(
    s3_ref: str,
    default_bucket: str,
    default_prefix: str = "",
) -> Tuple[str, str]:
    ref = (s3_ref or "").strip()
    if not ref:
        raise ValueError("Empty S3 reference")

    if ref.startswith("s3://"):
        parsed = urllib.parse.urlparse(ref)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Invalid S3 URI: {s3_ref}")
        return bucket, urllib.parse.unquote(key)

    if ref.startswith("http://") or ref.startswith("https://"):
        parsed = urllib.parse.urlparse(ref)
        host = parsed.netloc.lower()
        key = urllib.parse.unquote(parsed.path.lstrip("/"))

        if ".s3." in host:
            bucket = host.split(".s3.", 1)[0]
            if bucket and key:
                return bucket, key

        if host.startswith("s3."):
            parts = key.split("/", 1)
            if len(parts) == 2:
                return parts[0], parts[1]

        raise ValueError(f"Unsupported S3 URL format: {s3_ref}")

    key = ref.lstrip("/")
    prefix = (default_prefix or "").lstrip("/")
    if prefix and not key.startswith(prefix):
        key = f"{prefix}{key}"
    return default_bucket, key


def _load_transcript_payload(s3_ref: str) -> Dict[str, Any]:
    bucket, key = _resolve_s3_reference(
        s3_ref,
        default_bucket=TRANSCRIPT_EVENT_BUCKET,
        default_prefix=TRANSCRIPT_EVENT_PREFIX,
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return {"transcript": body.decode("utf-8", errors="ignore")}


def _consume_meeting_completed() -> None:
    print("🔧 Initializing Kafka consumer: meeting.completed")
    try:
        consumer = KafkaConsumer(
            "meeting.completed",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="rag-meeting-completed-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on: meeting.completed")
    except Exception as e:
        print(f"⚠️  Kafka consumer failed to start (meeting.completed): {e}")
        return

    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        event = message.value or {}
        booking_id = event.get("booking_id")
        s3_key = event.get("transcript_s3_key")
        if DEBUG_MEETING_COMPLETED_BOOKING_ID and booking_id == DEBUG_MEETING_COMPLETED_BOOKING_ID:
            print(f"🎯 meeting.completed consumed for target booking | booking_id={booking_id} | s3_key={s3_key}")
        if not booking_id or not s3_key:
            print(f"⚠️ Invalid meeting.completed payload: {event}")
            continue

        try:
            payload = _load_transcript_payload(s3_key)
            transcript_text = payload.get("transcript") or ""
            _kafka_publish(
                "transcript.saved",
                {
                    "booking_id": booking_id,
                    "student_email": payload.get("student_email") or event.get("student_email", ""),
                    "meeting_id": payload.get("meeting_id") or event.get("meeting_id", ""),
                    "transcript_s3_key": s3_key,
                    "transcript_chars": len(transcript_text),
                    "status": "SAVED",
                    "source": "rag_service",
                    "published_at": datetime.utcnow().isoformat(),
                },
            )
        except Exception as exc:
            print(f"❌ meeting.completed processing failed | {booking_id} | {exc}")

def get_student_rag_context(student_email: str, booking_reason: str = "") -> str:
    """
    Structured retrieval for suggestion pipeline.
    Combines email + booking_reason into a richer query than analyze_student_progress().
    Returns top-3 chunks as a single context string, empty string on failure.
    """
    query = f"student:{student_email} {booking_reason}".strip()
    hits = hybrid_search(query, top_k=3)
    if not hits:
        return ""
    return "\n\n".join(
        f"[Session {i+1}] {hit.get('text', '')}"
        for i, hit in enumerate(hits)
    )

def _consume_transcript_saved() -> None:
    print("🔧 Initializing Kafka consumer: transcript.saved")
    try:
        consumer = KafkaConsumer(
            "transcript.saved",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="rag-transcript-saved-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on: transcript.saved")
    except Exception as e:
        print(f"⚠️  Kafka consumer failed to start (transcript.saved): {e}")
        return

    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        event = message.value or {}
        booking_id = event.get("booking_id")
        s3_key = event.get("transcript_s3_key")
        if not booking_id or not s3_key:
            print(f"⚠️ Invalid transcript.saved payload: {event}")
            continue

        try:
            payload = _load_transcript_payload(s3_key)
            transcript_text = payload.get("transcript") or ""
            student_email = event.get("student_email") or payload.get("student_email") or ""
            ingest_result = ingest_transcript(
                student_email=student_email,
                transcript_text=transcript_text,
                booking_reason=event.get("booking_reason", ""),
                instructor_rating=event.get("instructor_rating"),
                booking_id=booking_id,
            )

            _kafka_publish(
                "embedding.created",
                {
                    "booking_id": booking_id,
                    "student_email": student_email,
                    "meeting_id": event.get("meeting_id") or payload.get("meeting_id", ""),
                    "transcript_s3_key": s3_key,
                    "chunks_created": ingest_result.get("chunks_created", 0),
                    "status": "COMPLETED",
                    "source": "rag_service",
                    "published_at": datetime.utcnow().isoformat(),
                },
            )
        except Exception as exc:
            print(f"❌ transcript.saved processing failed | {booking_id} | {exc}")


def _consume_embedding_created() -> None:
    print("🔧 Initializing Kafka consumer: embedding.created")
    try:
        consumer = KafkaConsumer(
            "embedding.created",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="rag-embedding-created-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on: embedding.created")
    except Exception as e:
        print(f"⚠️  Kafka consumer failed to start (embedding.created): {e}")
        return

    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        event = message.value or {}
        booking_id = event.get("booking_id")
        if not booking_id:
            print(f"⚠️ Invalid embedding.created payload: {event}")
            continue

        try:
            _kafka_publish(
                "rag.index.updated",
                {
                    "booking_id": booking_id,
                    "student_email": event.get("student_email", ""),
                    "meeting_id": event.get("meeting_id", ""),
                    "index_name": "meeting_intelligence",
                    "chunks_created": event.get("chunks_created", 0),
                    "status": "COMPLETED",
                    "source": "rag_service",
                    "published_at": datetime.utcnow().isoformat(),
                },
            )
            _kafka_publish(
                "student.learning.updated",
                {
                    "booking_id": booking_id,
                    "student_email": event.get("student_email", ""),
                    "meeting_id": event.get("meeting_id", ""),
                    "source": "rag_service",
                    "published_at": datetime.utcnow().isoformat(),
                },
            )
        except Exception as exc:
            print(f"❌ embedding.created processing failed | {booking_id} | {exc}")

# ═══════════════════════════════════════════════════════════════
# LAZY MODEL LOADERS
# Models are heavy; load on first use so the service starts fast.
# ═══════════════════════════════════════════════════════════════

_text_embedder   = None
_image_embedder  = None
_reranker        = None
_semantic_parser = None    # LlamaIndex SemanticSplitterNodeParser
_image_embedder_error: Optional[str] = None


def _get_text_embedder():
    """BGE-M3 via FlagEmbedding — used for both chunking & dense retrieval."""
    global _text_embedder
    if _text_embedder is None:
        print(f"📥 Loading text embedder: {TEXT_EMBED_MODEL}")
        from FlagEmbedding import BGEM3FlagModel
        _text_embedder = BGEM3FlagModel(TEXT_EMBED_MODEL, use_fp16=True)
        print("✅ BGE-M3 loaded")
    return _text_embedder


def _get_image_embedder():
    """nomic-embed-vision-v1.5 via sentence-transformers."""
    global _image_embedder, _image_embedder_error
    if _image_embedder is None:
        if _image_embedder_error is not None:
            raise RuntimeError(_image_embedder_error)
        print(f"📥 Loading image embedder: {IMAGE_EMBED_MODEL}")
        try:
            from sentence_transformers import SentenceTransformer
            _image_embedder = SentenceTransformer(
                "nomic-ai/nomic-embed-vision-v1.5",
                trust_remote_code=True,
            )
        except Exception as e:
            _image_embedder_error = (
                f"Image embedder unavailable: {e}. "
                "Install required extras, e.g. `pip install einops`."
            )
            raise RuntimeError(_image_embedder_error) from e
        print("✅ nomic-embed-vision-v1.5 loaded")
    return _image_embedder


def _get_reranker():
    """BAAI/bge-reranker-v2-m3 via FlagEmbedding."""
    global _reranker
    if _reranker is None:
        print(f"📥 Loading reranker: {RERANKER_MODEL}")
        from FlagEmbedding import FlagReranker
        _reranker = FlagReranker(RERANKER_MODEL, use_fp16=True)
        print("✅ BGE-Reranker-v2-m3 loaded")
    return _reranker


def _get_semantic_parser():
    """
    LlamaIndex SemanticSplitterNodeParser backed by a thin HuggingFace
    wrapper around BGE-M3.  This is the FRAMEWORK-level chunker.
    """
    global _semantic_parser
    if _semantic_parser is None:
        print("📥 Building LlamaIndex SemanticSplitterNodeParser …")
        from llama_index.core.node_parser import SemanticSplitterNodeParser
        from llama_index.embeddings.huggingface import HuggingFaceEmbedding

        # Use a lighter HuggingFace wrapper for LlamaIndex (it doesn't need
        # sparse/colbert — those come from FlagEmbedding separately).
        hf_embed = HuggingFaceEmbedding(
            model_name=TEXT_EMBED_MODEL,
            trust_remote_code=True,
            embed_batch_size=16,
        )
        _semantic_parser = SemanticSplitterNodeParser(
            embed_model=hf_embed,
            breakpoint_percentile_threshold=int((1.0 - SEMANTIC_THRESHOLD) * 100),
            buffer_size=SENTENCE_WINDOW,
        )
        print("✅ LlamaIndex SemanticSplitterNodeParser ready")
    return _semantic_parser


# ═══════════════════════════════════════════════════════════════
# EMBEDDING HELPERS
# ═══════════════════════════════════════════════════════════════

def embed_text(texts: List[str]) -> Dict:
    """
    Dense + sparse embeddings via BGE-M3.
    Returns dict with 'dense_vecs' (ndarray [N,1024]) and 'lexical_weights'.
    """
    model = _get_text_embedder()
    return model.encode(
        texts,
        batch_size=8,
        max_length=512,
        return_dense=True,
        return_sparse=True,
        return_colbert_vecs=False,
    )


def embed_image(pil_image) -> np.ndarray:
    """768-dim embedding for a PIL image via nomic-embed-vision-v1.5."""
    model = _get_image_embedder()
    return model.encode([pil_image])[0]


def embed_image_from_bytes(image_bytes: bytes) -> Optional[np.ndarray]:
    global _image_embedder_error
    if _image_embedder_error is not None:
        return None
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        return embed_image(img)
    except Exception as e:
        if _image_embedder_error is None:
            _image_embedder_error = str(e)
            print(f"⚠️  Image embedding disabled: {_image_embedder_error}")
        return None


# ═══════════════════════════════════════════════════════════════
# SEMANTIC CHUNKING  (LlamaIndex framework)
# ═══════════════════════════════════════════════════════════════

def semantic_chunk(text: str) -> List[str]:
    """
    Split *text* into semantically coherent chunks using LlamaIndex
    SemanticSplitterNodeParser (BGE-M3 embeddings under the hood).

    Falls back to a simple sentence-level splitter if LlamaIndex is
    unavailable or the text is too short.

    Post-processing enforces MIN/MAX_CHUNK_WORDS limits.
    """
    text = text.strip()
    if not text:
        return []

    # ── Try LlamaIndex SemanticSplitter ──────────────────────
    try:
        from llama_index.core import Document as LIDocument
        parser  = _get_semantic_parser()
        li_doc  = LIDocument(text=text)
        nodes   = parser.get_nodes_from_documents([li_doc])
        raw_chunks = [n.get_content() for n in nodes if n.get_content().strip()]
    except Exception as e:
        print(f"⚠️  LlamaIndex SemanticSplitter failed, using fallback: {e}")
        raw_chunks = _fallback_semantic_chunk(text)

    # ── Enforce word-count limits ─────────────────────────────
    final: List[str] = []
    for chunk in raw_chunks:
        words = chunk.split()
        if len(words) < MIN_CHUNK_WORDS:
            if final:
                final[-1] += " " + chunk
            else:
                final.append(chunk)
        elif len(words) > MAX_CHUNK_WORDS:
            step = MAX_CHUNK_WORDS - max(10, MAX_CHUNK_WORDS // 10)
            for start in range(0, len(words), step):
                sub = " ".join(words[start: start + MAX_CHUNK_WORDS]).strip()
                if sub:
                    final.append(sub)
        else:
            final.append(chunk)

    return [c.strip() for c in final if c.strip()]


def _fallback_semantic_chunk(text: str) -> List[str]:
    """
    Pure-numpy semantic chunker used if LlamaIndex is unavailable.
    Embeds sliding sentence windows; splits where cosine similarity
    drops below SEMANTIC_THRESHOLD.
    """
    sentences = _split_sentences(text)
    if not sentences:
        return []
    if len(sentences) <= SENTENCE_WINDOW:
        return [" ".join(sentences)]

    windows = [
        " ".join(sentences[i: i + SENTENCE_WINDOW])
        for i in range(len(sentences) - SENTENCE_WINDOW + 1)
    ]
    model  = _get_text_embedder()
    enc    = model.encode(windows, batch_size=16, max_length=256,
                          return_dense=True, return_sparse=False,
                          return_colbert_vecs=False)
    vecs   = enc["dense_vecs"]

    boundaries = [0]
    for i in range(1, len(vecs)):
        sim = _cosine(vecs[i - 1], vecs[i])
        if sim < SEMANTIC_THRESHOLD:
            boundaries.append(i)
    boundaries.append(len(sentences))

    chunks = []
    for i in range(len(boundaries) - 1):
        chunk = " ".join(sentences[boundaries[i]: boundaries[i + 1]])
        chunks.append(chunk)
    return chunks


def _split_sentences(text: str) -> List[str]:
    raw = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences, buf = [], ""
    for s in raw:
        buf = (buf + " " + s).strip() if buf else s
        if len(buf.split()) >= 5:
            sentences.append(buf)
            buf = ""
    if buf:
        sentences.append(buf)
    return sentences


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / denom) if denom > 1e-9 else 0.0


# ═══════════════════════════════════════════════════════════════
# S3 CLIENT & HELPERS
# ═══════════════════════════════════════════════════════════════

s3 = boto3.client("s3", region_name=AWS_REGION)


def list_s3_objects() -> List[Dict]:
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for item in page.get("Contents", []):
            key = item.get("Key", "")
            if not key or key.endswith("/") or item.get("Size", 0) == 0:
                continue
            objects.append({
                "key":  key,
                "etag": (item.get("ETag") or "").strip('"'),
                "size": item.get("Size", 0),
            })
    return sorted(objects, key=lambda x: x["key"])


def download_s3_bytes(key: str) -> bytes:
    bucket, resolved_key = _resolve_s3_reference(key, default_bucket=S3_BUCKET)
    return s3.get_object(Bucket=bucket, Key=resolved_key)["Body"].read()


def _has_existing_doc_index() -> bool:
    try:
        return redis_vec.scard(DOC_IDX_KEY) > 0 or redis_vec.exists(ETAG_KEY) == 1
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
# TEXT EXTRACTION
# ═══════════════════════════════════════════════════════════════

def extract_text(payload: bytes, key: str) -> str:
    ext = os.path.splitext(key.lower())[1]
    if ext in SUPPORTED_TEXT_EXT:
        return payload.decode("utf-8", errors="ignore")
    if ext in SUPPORTED_DOCX_EXT:
        return _extract_docx(payload)
    if ext in SUPPORTED_PDF_EXT:
        return _extract_pdf(payload)
    raise ValueError(f"Unsupported text extension: {ext}")


def _extract_docx(payload: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        xml_bytes = zf.read("word/document.xml")
    root = ET.fromstring(xml_bytes)
    ns   = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paras = []
    for para in root.findall(".//w:p", ns):
        runs = [n.text for n in para.findall(".//w:t", ns) if n.text]
        line = "".join(runs).strip()
        if line:
            paras.append(line)
    return "\n".join(paras)


def _extract_pdf(payload: bytes) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(payload))
        pages  = [p.extract_text() or "" for p in reader.pages]
        return "\n".join(p.strip() for p in pages if p.strip())
    except Exception as e:
        print(f"⚠️  pypdf failed: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════
# IMAGE EXTRACTION FROM DOCX / PDF
# ═══════════════════════════════════════════════════════════════

def extract_images_from_docx(payload: bytes) -> List[bytes]:
    images = []
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            for name in zf.namelist():
                if name.startswith("word/media/") and any(
                    name.lower().endswith(ext)
                    for ext in [".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp"]
                ):
                    images.append(zf.read(name))
    except Exception as e:
        print(f"⚠️  DOCX image extraction failed: {e}")
    return images


def extract_images_from_pdf(payload: bytes) -> List[bytes]:
    images = []
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(payload))
        for page in reader.pages:
            for img_obj in page.images:
                images.append(img_obj.data)
    except Exception as e:
        print(f"⚠️  PDF image extraction failed: {e}")
    return images


# ═══════════════════════════════════════════════════════════════
# REDIS VECTOR STORE HELPERS
# ═══════════════════════════════════════════════════════════════

def _vec_to_bytes(vec: np.ndarray) -> bytes:
    """Pack float32 ndarray → little-endian bytes (Redis-storable)."""
    return struct.pack(f"<{len(vec)}f", *vec.astype(np.float32))


def _bytes_to_vec(b: bytes, dim: int) -> np.ndarray:
    return np.array(struct.unpack(f"<{dim}f", b), dtype=np.float32)


def create_redis_vector_index() -> None:
    """
    Create a flat HNSW vector index in Redis using raw commands if
    redis-py's Search module is available, otherwise skip (manual HNSW
    search will work without the index for small corpora).
    """
    try:
        from redis.commands.search.field import VectorField, TextField, NumericField
        from redis.commands.search.indexDefinition import IndexDefinition, IndexType
        from redis.commands.search import Query as RediSearch_Query

        idx_name = "rag_vec_idx"
        try:
            redis_vec.ft(idx_name).info()
            print(f"ℹ️  Redis vector index '{idx_name}' already exists")
            return
        except Exception:
            pass  # index doesn't exist yet — create it

        schema = (
            TextField("$.filename", as_name="filename"),
            TextField("$.modality", as_name="modality"),
            VectorField(
                "$.dense_vec",
                "HNSW",
                {
                    "TYPE":            "FLOAT32",
                    "DIM":             TEXT_EMBED_DIM,
                    "DISTANCE_METRIC": "COSINE",
                    "M":               16,
                    "EF_CONSTRUCTION": 200,
                },
                as_name="dense_vec",
            ),
        )
        redis_vec.ft(idx_name).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[CHUNK_PFX],
                index_type=IndexType.JSON,
            ),
        )
        print(f"✅ Redis HNSW vector index '{idx_name}' created")
    except Exception as e:
        print(f"⚠️  Redis vector index creation skipped (will use brute-force KNN): {e}")


def store_chunk_vector(
    chunk_id:      str,
    doc_id:        str,
    text:          str,
    filename:      str,
    dense_vec:     np.ndarray,
    chunk_idx:     int,
    modality:      str = "text",
    image_caption: str = "",
    booking_id:    str = "",            
) -> None:
    """Store one chunk with its dense vector in Redis hash (CHUNK_PFX + chunk_id)."""
    key = f"{CHUNK_PFX}{chunk_id}"
    redis_vec.hset(key, mapping={
        "doc_id":        doc_id,
        "text":          text.encode("utf-8"),
        "filename":      filename.encode("utf-8"),
        "modality":      modality.encode("utf-8"),
        "chunk_idx":     str(chunk_idx).encode("utf-8"),
        "image_caption": image_caption.encode("utf-8"),
        "dense_vec":     _vec_to_bytes(dense_vec),
        "booking_id":    booking_id.encode("utf-8"),
    })
    redis_vec.sadd(DOC_IDX_KEY, doc_id)


def delete_doc_chunks(doc_id: str) -> int:
    """Delete all chunks belonging to doc_id from Vec DB and BM25 DB."""
    deleted = 0
    for key in redis_vec.scan_iter(f"{CHUNK_PFX}*"):
        raw = redis_vec.hget(key, "doc_id")
        stored_id = raw.decode() if isinstance(raw, bytes) else (raw or "")
        if stored_id == doc_id:
            chunk_id = key.decode().replace(CHUNK_PFX, "") if isinstance(key, bytes) else key.replace(CHUNK_PFX, "")
            redis_vec.delete(key)
            # Remove from BM25 index
            redis_bm25_str.hdel(BM25_META_KEY, chunk_id)
            deleted += 1
    redis_vec.srem(DOC_IDX_KEY, doc_id)
    return deleted


# ═══════════════════════════════════════════════════════════════
# BM25 INVERTED INDEX  (Redis-backed sparse retrieval)
# ═══════════════════════════════════════════════════════════════

_STOPWORDS = {
    "a","an","and","are","as","at","be","been","by","for","from",
    "has","he","in","is","it","its","of","on","that","the","to",
    "was","were","will","with","or","but",
}

def _tokenize(text: str) -> List[str]:
    tokens = re.findall(r'\b[a-zA-Z0-9_]{2,}\b', text.lower())
    return [t for t in tokens if t not in _STOPWORDS]


def bm25_index_chunk(chunk_id: str, text: str) -> None:
    """
    Index one chunk into the BM25 inverted index.
      rag:bm25:term:<term> → ZSET {chunk_id: normalized_tf}
      rag:bm25:meta        → HASH {chunk_id: doc_len}
    """
    tokens = _tokenize(text)
    if not tokens:
        return
    tf: Dict[str, int] = defaultdict(int)
    for t in tokens:
        tf[t] += 1
    doc_len = len(tokens)
    redis_bm25_str.hset(BM25_META_KEY, chunk_id, str(doc_len))
    for term, count in tf.items():
        redis_bm25_str.zadd(f"{BM25_TERM_PFX}{term}", {chunk_id: count / doc_len})


def bm25_search(query: str, top_k: int = HYBRID_BM25_TOP_K) -> List[Tuple[str, float]]:
    """
    Robertson BM25 (k1=1.5, b=0.75).
    Returns [(chunk_id, bm25_score), …] sorted descending.
    """
    k1, b = 1.5, 0.75
    terms = _tokenize(query)
    if not terms:
        return []

    # Corpus stats
    meta_all   = redis_bm25_str.hgetall(BM25_META_KEY)
    if not meta_all:
        return []
    doc_lens   = {k: int(v) for k, v in meta_all.items()}
    N          = len(doc_lens)
    avgdl      = sum(doc_lens.values()) / N if N else 1.0

    candidate_scores: Dict[str, float] = defaultdict(float)

    for term in set(terms):
        tf_map = redis_bm25_str.zrangebyscore(
            f"{BM25_TERM_PFX}{term}", "-inf", "+inf", withscores=True,
        )
        if not tf_map:
            continue

        df = len(tf_map)
        idf = math.log(
        (N - df + 0.5) / (df + 0.5) + 1
        )

        for chunk_id, tf_norm in tf_map:
            dl = doc_lens.get(chunk_id, avgdl)
            tf_bm25 = (
                tf_norm * (k1 + 1)
                / (tf_norm + k1 * (1 - b + b * dl / avgdl))
            )
            candidate_scores[chunk_id] += idf * tf_bm25

    return sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]


# ═══════════════════════════════════════════════════════════════
# DENSE KNN SEARCH
# ═══════════════════════════════════════════════════════════════

def dense_search(query: str, top_k: int = HYBRID_DENSE_TOP_K) -> List[Tuple[str, float]]:
    """
    Brute-force cosine KNN over all stored dense vectors.
    Uses LlamaIndex VectorStoreIndex if available; falls back to pure numpy.
    """
    enc   = embed_text([query])
    qvec  = enc["dense_vecs"][0]

    scores: List[Tuple[str, float]] = []
    for key in redis_vec.scan_iter(f"{CHUNK_PFX}*"):
        raw_vec = redis_vec.hget(key, "dense_vec")
        if raw_vec is None:
            continue
        try:
            dvec = _bytes_to_vec(raw_vec, TEXT_EMBED_DIM)
        except Exception:
            continue
        sim      = _cosine(qvec, dvec)
        chunk_id = (key.decode() if isinstance(key, bytes) else key).replace(CHUNK_PFX, "")
        scores.append((chunk_id, sim))

    return sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]


# ═══════════════════════════════════════════════════════════════
# HYBRID SEARCH  +  RERANKING
# ═══════════════════════════════════════════════════════════════

def hybrid_search(
    query:  str,
    top_k:  int   = RERANK_TOP_K,
    alpha:  float = HYBRID_ALPHA,
) -> List[Dict]:
    """
    Hybrid Search + Reranking pipeline (LlamaIndex-style):

      1. Dense KNN  (BGE-M3 embeddings, cosine)
      2. BM25 sparse search
      3. Reciprocal Rank Fusion (RRF) to merge ranked lists
      4. Fetch chunk text for top candidates
      5. BAAI/bge-reranker-v2-m3 cross-encoder reranking
      6. Return top_k chunks

    alpha controls the RRF weighting:
      alpha=1.0  → only dense scores contribute
      alpha=0.0  → only BM25 scores contribute
    """
    # ── 1. Dense KNN ────────────────────────────────────────────
    dense_hits = dense_search(query, top_k=HYBRID_DENSE_TOP_K)

    # ── 2. BM25 sparse search ────────────────────────────────────
    bm25_hits  = bm25_search(query, top_k=HYBRID_BM25_TOP_K)

    # ── 3. RRF fusion ───────────────────────────────────────────
    K   = 60    # RRF constant
    rrf: Dict[str, float] = defaultdict(float)

    for rank, (cid, _) in enumerate(dense_hits):
        rrf[cid] += alpha * (1.0 / (K + rank + 1))
    for rank, (cid, _) in enumerate(bm25_hits):
        rrf[cid] += (1 - alpha) * (1.0 / (K + rank + 1))

    candidates = sorted(rrf.items(), key=lambda x: x[1], reverse=True)[: top_k * 2]
    if not candidates:
        return []

    # ── 4. Fetch chunk texts ─────────────────────────────────────
    def _d(raw: Dict, k: str) -> str:
        if not raw:
            return ""
        first_key = next(iter(raw.keys()))
        lookup_key = (
            k.encode()
            if isinstance(first_key, bytes)
            else k
        )
        v = raw.get(lookup_key, b"")
        return (
            v.decode("utf-8", errors="ignore")
            if isinstance(v, bytes)
            else str(v)
        )

    candidate_docs: List[Dict] = []
    for chunk_id, rrf_score in candidates:
        raw = redis_vec.hgetall(f"{CHUNK_PFX}{chunk_id}")
        if not raw:
            continue
        candidate_docs.append({
            "chunk_id":     chunk_id,
            "text":         _d(raw, "text"),
            "doc_id":       _d(raw, "doc_id"),
            "filename":     _d(raw, "filename"),
            "modality":     _d(raw, "modality"),
            "chunk_idx":    _d(raw, "chunk_idx"),
            "image_caption":_d(raw, "image_caption"),
            "rrf_score":    rrf_score,
        })

    if not candidate_docs:
        return []

    # ── 5. BGE-Reranker-v2-m3 ───────────────────────────────────
    try:
        reranker = _get_reranker()
        pairs    = [[query, d["text"]] for d in candidate_docs]
        scores   = reranker.compute_score(pairs, normalize=True)
        if not isinstance(scores, list):
            scores = [scores]
        for doc, score in zip(candidate_docs, scores):
            doc["rerank_score"] = float(score)
        candidate_docs.sort(key=lambda x: x["rerank_score"], reverse=True)
    except Exception as e:
        print(f"⚠️  Reranker failed, falling back to RRF order: {e}")
        for doc in candidate_docs:
            doc["rerank_score"] = doc["rrf_score"]

    return candidate_docs[:top_k]


# ═══════════════════════════════════════════════════════════════
# INGESTION PIPELINE
# ═══════════════════════════════════════════════════════════════

def generate_doc_id(s3_key: str) -> str:
    return hashlib.md5(s3_key.encode()).hexdigest()


def generate_chunk_id(doc_id: str, idx: int, text: str) -> str:
    return hashlib.sha256(f"{doc_id}:{idx}:{text[:64]}".encode()).hexdigest()[:24]


def ingest_text_document(payload: bytes, s3_key: str, doc_id: str) -> int:
    """
    Full text ingestion pipeline (LlamaIndex-powered):
      1. Extract raw text
      2. SemanticSplitterNodeParser → semantic chunks
      3. BGE-M3 dense + sparse embedding (batch)
      4. Store in Redis Vec DB + BM25 index
    Returns number of chunks stored.
    """
    filename = os.path.basename(s3_key)
    try:
        text = extract_text(payload, s3_key)
    except Exception as e:
        print(f"⚠️  Text extraction failed {s3_key}: {e}")
        return 0

    if not text.strip():
        print(f"⚠️  No text extracted from {s3_key}")
        return 0

    chunks = semantic_chunk(text)   # ← LlamaIndex SemanticSplitterNodeParser
    if not chunks:
        print(f"⚠️  No semantic chunks from {s3_key}")
        return 0

    print(f"  📝 {filename}: {len(chunks)} semantic chunks → embedding …")

    enc        = embed_text(chunks)
    dense_vecs = enc["dense_vecs"]   # [N, 1024]

    stored = 0
    for idx, (chunk_text, dvec) in enumerate(zip(chunks, dense_vecs)):
        chunk_id = generate_chunk_id(doc_id, idx, chunk_text)
        store_chunk_vector(
            chunk_id=chunk_id, doc_id=doc_id, text=chunk_text,
            filename=filename, dense_vec=dvec,
            chunk_idx=idx, modality="text",
        )
        bm25_index_chunk(chunk_id, chunk_text)
        stored += 1

    print(f"  ✅ {filename}: {stored} text chunks stored")
    return stored


def ingest_image_document(payload: bytes, s3_key: str, doc_id: str,
                           chunk_idx_offset: int = 0) -> int:
    """
    Standalone image ingestion:
      1. nomic-embed-vision-v1.5 → 768-dim
      2. Zero-pad to 1024 for unified vector index
      3. Store with modality='image'
    """
    filename = os.path.basename(s3_key)
    vec_768  = embed_image_from_bytes(payload)
    if vec_768 is None:
        return 0

    padded = np.zeros(TEXT_EMBED_DIM, dtype=np.float32)
    padded[:len(vec_768)] = vec_768

    chunk_id = generate_chunk_id(doc_id, chunk_idx_offset, filename)
    caption  = f"[Image from {filename}]"
    store_chunk_vector(
        chunk_id=chunk_id, doc_id=doc_id, text=caption,
        filename=filename, dense_vec=padded,
        chunk_idx=chunk_idx_offset, modality="image",
        image_caption=caption,
    )
    bm25_index_chunk(chunk_id, caption)
    print(f"  🖼️  {filename}: image embedding stored (768→1024 padded)")
    return 1


def ingest_embedded_images(payload: bytes, s3_key: str, doc_id: str,
                            chunk_idx_offset: int) -> int:
    """Extract and embed images embedded inside DOCX or PDF."""
    ext = os.path.splitext(s3_key.lower())[1]
    images: List[bytes] = []
    if ext == ".docx":
        images = extract_images_from_docx(payload)
    elif ext == ".pdf":
        images = extract_images_from_pdf(payload)

    stored = 0
    for i, img_bytes in enumerate(images):
        vec_768 = embed_image_from_bytes(img_bytes)
        if vec_768 is None:
            continue
        padded = np.zeros(TEXT_EMBED_DIM, dtype=np.float32)
        padded[:len(vec_768)] = vec_768
        chunk_id = generate_chunk_id(doc_id, chunk_idx_offset + i, f"image_{i}")
        caption  = f"[Embedded image {i+1} from {os.path.basename(s3_key)}]"
        store_chunk_vector(
            chunk_id=chunk_id, doc_id=doc_id, text=caption,
            filename=os.path.basename(s3_key), dense_vec=padded,
            chunk_idx=chunk_idx_offset + i, modality="image",
            image_caption=caption,
        )
        bm25_index_chunk(chunk_id, caption)
        stored += 1

    if stored:
        print(f"  🖼️  {os.path.basename(s3_key)}: {stored} embedded image(s) stored")
    return stored


def full_ingest_pipeline(s3_key: str, extra_metadata: Optional[Dict] = None) -> Dict:
    """
    Complete ingestion for one S3 object via LlamaIndex-powered chunking.
    Handles text, PDF, DOCX, and standalone images.
    """
    doc_id   = generate_doc_id(s3_key)
    filename = os.path.basename(s3_key)
    ext      = os.path.splitext(s3_key.lower())[1]
    print(f"⚙️  Ingesting: {s3_key}")
    payload  = download_s3_bytes(s3_key)

    text_chunks = image_chunks = 0

    if ext in SUPPORTED_IMAGE_EXT:
        image_chunks = ingest_image_document(payload, s3_key, doc_id)
    else:
        text_chunks  = ingest_text_document(payload, s3_key, doc_id)
        image_chunks = ingest_embedded_images(payload, s3_key, doc_id, text_chunks)

    return {
        "doc_id":       doc_id,
        "s3_key":       s3_key,
        "filename":     filename,
        "text_chunks":  text_chunks,
        "image_chunks": image_chunks,
        "total_chunks": text_chunks + image_chunks,
        "timestamp":    datetime.now().isoformat(),
    }


def full_sync_from_s3(force: bool = False) -> Dict:
    """
    Sync all S3 objects into the vector + BM25 indexes.
    Skips unchanged files (etag match) unless force=True.
    """
    objects = list_s3_objects()
    print(f"📦 Found {len(objects)} objects in s3://{S3_BUCKET}/{S3_PREFIX}")

    results = {"ingested": 0, "skipped": 0, "failed": 0, "docs": []}
    all_ext = (SUPPORTED_TEXT_EXT | SUPPORTED_DOCX_EXT |
               SUPPORTED_PDF_EXT  | SUPPORTED_IMAGE_EXT)

    for item in objects:
        key, etag = item["key"], item["etag"]
        if os.path.splitext(key.lower())[1] not in all_ext:
            results["skipped"] += 1
            continue

        stored_etag = redis_vec.hget(ETAG_KEY, key)
        stored_etag_str = stored_etag.decode() if stored_etag else None
        if not force and stored_etag_str == etag:
            print(f"  ⏭️  Unchanged: {key}")
            results["skipped"] += 1
            continue

        delete_doc_chunks(generate_doc_id(key))
        try:
            result = full_ingest_pipeline(key)
            redis_vec.hset(ETAG_KEY, key, etag)
            results["ingested"] += 1
            results["docs"].append(result)
        except Exception as e:
            print(f"  ❌ Failed to ingest {key}: {e}")
            traceback.print_exc()
            results["failed"] += 1

    print(f"✅ Sync done: ingested={results['ingested']}  "
          f"skipped={results['skipped']}  failed={results['failed']}")
    return results

# =========================================================
# MAIN RAG FUNCTION
# =========================================================

def analyze_student_progress(student_email: str):
    query = f"{student_email} {DEFAULT_PROGRESS_QUERY}"
    hits = hybrid_search(query, top_k=5)
    if not hits:
        return "No indexed transcript found for this student yet."
    return "\n\n".join(
        f"[{hit.get('filename', 'unknown')}] {hit.get('text', '')}"
        for hit in hits
    )

# =========================================================
# INGEST TRANSCRIPT
# =========================================================
def ingest_transcript(
    student_email,
    transcript_text,
    booking_reason,
    instructor_rating=None,
    booking_id=None,
):
    combined_text = f"""

    STUDENT EMAIL:
    {student_email}

    BOOKING REASON:
    {booking_reason}

    TEAMS TRANSCRIPT:
    {transcript_text}

    INSTRUCTOR RATING:
    {instructor_rating}

    """

    doc_id = hashlib.md5(f"{student_email}:{booking_reason}:{transcript_text}".encode()).hexdigest()
    filename = f"transcript_{student_email or 'unknown'}"
    chunks = semantic_chunk(combined_text)
    if not chunks:
        return {"chunks_created": 0, "status": "empty"}

    enc = embed_text(chunks)
    dense_vecs = enc["dense_vecs"]
    for idx, (chunk_text, dense_vec) in enumerate(zip(chunks, dense_vecs)):
        chunk_id = generate_chunk_id(doc_id, idx, chunk_text)
        store_chunk_vector(
            chunk_id=chunk_id,
            doc_id=doc_id,
            text=chunk_text,
            filename=filename,
            dense_vec=dense_vec,
            chunk_idx=idx,
            modality="text",
            booking_id=booking_id or "",
        )
        bm25_index_chunk(chunk_id, chunk_text)

    return {
        "chunks_created": len(chunks),
        "status": "success"
    }

def ingest_all_documents(force: bool = False) -> Dict:
    return full_sync_from_s3(force=force)


# ═══════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ═══════════════════════════════════════════════════════════════

class IngestRequest(BaseModel):
    s3_key:   str
    force:    bool = False
    metadata: Optional[Dict[str, Any]] = None


class SyncRequest(BaseModel):
    force: bool = False


class RetrieveRequest(BaseModel):
    query: str
    top_k: int   = Field(default=RERANK_TOP_K, ge=1, le=20)
    alpha: float = Field(default=HYBRID_ALPHA, ge=0.0, le=1.0)


class DeleteRequest(BaseModel):
    s3_key: str


# ═══════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.get("/")
def root():
    try:
        redis_vec.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "service":    "Multimodal RAG Service v3 (LlamaIndex)",
        "status":     "running",
        "framework":  "llama-index-core ≥ 0.10",
        "chunking":   "SemanticSplitterNodeParser (BGE-M3)",
        "text_model": TEXT_EMBED_MODEL,
        "image_model":IMAGE_EMBED_MODEL,
        "reranker":   RERANKER_MODEL,
        "retrieval":  "Hybrid (dense KNN + BM25 RRF) + BGE-Reranker-v2-m3",
        "llm":        OLLAMA_MODEL,
        "redis_ok":   redis_ok,
        "s3_bucket":  S3_BUCKET,
        "s3_prefix":  S3_PREFIX,
    }


@app.get("/health")
def health():
    checks: Dict[str, str] = {}
    for client, label in [(redis_vec, "redis_vec"), (redis_bm25_str, "redis_bm25")]:
        try:
            client.ping()
            checks[label] = "ok"
        except Exception as e:
            checks[label] = f"error: {e}"
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        checks["s3"] = "ok"
    except Exception as e:
        checks["s3"] = f"error: {e}"
    ok = all(v == "ok" for v in checks.values())
    return {"status": "healthy" if ok else "degraded", "checks": checks}


@app.post("/ingest/manual")
async def ingest_manual(req: IngestRequest):
    """Ingest a single S3 document (blocking)."""
    try:
        if req.force:
            delete_doc_chunks(generate_doc_id(req.s3_key))
        result = full_ingest_pipeline(req.s3_key, req.metadata)
        return {"success": True, **result}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ingest/sync")
async def ingest_sync(req: SyncRequest, background_tasks: BackgroundTasks):
    """Trigger full S3 sync in the background."""
    background_tasks.add_task(full_sync_from_s3, req.force)
    return {"success": True, "message": "Sync started in background", "force": req.force}


@app.post("/ingest/s3-event")
async def s3_event_handler(event: dict, background_tasks: BackgroundTasks):
    """Handle S3 event notification (SNS/SQS trigger)."""
    key = event.get("key") or event.get("s3_key")
    if not key:
        raise HTTPException(status_code=400, detail="Missing 's3_key' in event")
    background_tasks.add_task(full_ingest_pipeline, key)
    return {"success": True, "message": f"Ingestion queued for {key}"}


@app.post("/retrieve")
def retrieve(req: RetrieveRequest):
    """
    Primary retrieval endpoint called by chatbot_service.
    Returns top_k chunks via Hybrid Search + BGE-Reranker-v2-m3.
    """
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Empty query")
    try:
        hits = hybrid_search(req.query, top_k=req.top_k, alpha=req.alpha)
        return {
            "success": True,
            "query":   req.query,
            "results": hits,
            "count":   len(hits),
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/documents")
def delete_document(req: DeleteRequest):
    doc_id  = generate_doc_id(req.s3_key)
    deleted = delete_doc_chunks(doc_id)
    redis_vec.hdel(ETAG_KEY, req.s3_key)
    return {"success": True, "s3_key": req.s3_key, "chunks_deleted": deleted}


@app.get("/stats")
def stats():
    try:
        doc_count   = redis_vec.scard(DOC_IDX_KEY)
        chunk_count = sum(1 for _ in redis_vec.scan_iter(f"{CHUNK_PFX}*"))
        bm25_terms  = sum(1 for _ in redis_bm25_str.scan_iter(f"{BM25_TERM_PFX}*"))
        return {
            "success":          True,
            "framework":        "llama-index SemanticSplitterNodeParser",
            "total_documents":  doc_count,
            "total_chunks":     chunk_count,
            "bm25_terms":       bm25_terms,
            "text_embed_dim":   TEXT_EMBED_DIM,
            "image_embed_dim":  IMAGE_EMBED_DIM,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rag-context")
def rag_context_endpoint(req: dict):
    """Called by meeting_service.py during /meeting/agentic-suggestions."""
    email = req.get("student_email", "")
    reason = req.get("booking_reason", "")
    context = get_student_rag_context(email, reason)
    return {"context": context, "found": bool(context)}

# ═══════════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    print("=" * 65)
    print("🚀 Multimodal RAG Service v3  (LlamaIndex-powered)")
    print(f"   Framework    : llama-index-core ≥ 0.10")
    print(f"   Chunking     : SemanticSplitterNodeParser  (BGE-M3 backbone)")
    print(f"   Text model   : {TEXT_EMBED_MODEL}")
    print(f"   Image model  : {IMAGE_EMBED_MODEL}")
    print(f"   Reranker     : {RERANKER_MODEL}")
    print(f"   LLM          : {OLLAMA_MODEL}  via Ollama")
    print(f"   Retrieval    : Hybrid KNN+BM25 RRF → BGE-Reranker-v2-m3")
    print(f"   Redis RAG    : {REDIS_HOST}:{REDIS_RAG_PORT}"
          f"  Vec=db{REDIS_VEC_DB}  BM25=db{REDIS_BM25_DB}")
    print(f"   S3           : s3://{S3_BUCKET}/{S3_PREFIX}")
    print("=" * 65)

    create_redis_vector_index()

    should_ingest = AUTO_INGEST_ON_STARTUP and (AUTO_INGEST_FORCE or not _has_existing_doc_index())

    if should_ingest:
        print(f"📥 Running startup ingestion (force={AUTO_INGEST_FORCE}) …")
        try:
            r = ingest_all_documents(force=AUTO_INGEST_FORCE)
            print(f"✅ Ingestion done: ingested={r.get('ingested',0)}"
                  f"  skipped={r.get('skipped',0)}  failed={r.get('failed',0)}")
        except Exception as e:
            print(f"❌ Startup ingestion failed: {e}")
    elif AUTO_INGEST_ON_STARTUP:
        print("⏭️  Startup ingestion skipped — existing RAG index detected")
    else:
        print("⏭️  AUTO_INGEST_ON_STARTUP=false — skipping")

    if ENABLE_KAFKA_PIPELINE:
        t_meeting_completed = threading.Thread(
            target=_consume_meeting_completed,
            daemon=True,
            name="kafka-meeting-completed",
        )
        t_meeting_completed.start()
        print("🚀 Kafka consumer thread started: meeting.completed")

        t_transcript_saved = threading.Thread(
            target=_consume_transcript_saved,
            daemon=True,
            name="kafka-transcript-saved",
        )
        t_transcript_saved.start()
        print("🚀 Kafka consumer thread started: transcript.saved")

        t_embedding_created = threading.Thread(
            target=_consume_embedding_created,
            daemon=True,
            name="kafka-embedding-created",
        )
        t_embedding_created.start()
        print("🚀 Kafka consumer thread started: embedding.created")
        print("🚀 Kafka pipeline consumers started: meeting.completed, transcript.saved, embedding.created")
    else:
        print("⏭️  ENABLE_KAFKA_PIPELINE=false — skipping Kafka consumers")


@app.on_event("shutdown")
async def shutdown():
    print("🛑 RAG Service shutting down")
    redis_vec.close()
    redis_bm25_str.close()

# =========================================================
# SEMANTIC CHUNKING
# =========================================================
splitter = SemanticSplitterNodeParser(
    buffer_size=1,
    breakpoint_percentile_threshold=95,
    embed_model=HuggingFaceEmbedding(model_name="BAAI/bge-m3")
)

# =========================================================
# RERANKER
# =========================================================
reranker = FlagEmbeddingReranker(
    model="BAAI/bge-reranker-v2-m3",
    top_n=5
)

# =========================================================
# HARDCODED STUDENT PROGRESS QUERY
# =========================================================

DEFAULT_PROGRESS_QUERY = """
Please identify the student's progress.

Analyze:
- learning improvement
- repeated weaknesses
- difficult topics
- confidence growth
- practical understanding
- consistency
- trainer observations

Provide concise tutor guidance.
"""

# ═══════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("rag_service:app", host="0.0.0.0", port=7900, reload=False)
