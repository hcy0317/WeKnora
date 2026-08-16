import asyncio
import os
import time
from typing import Any

import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sentence_transformers import CrossEncoder


MODEL_ID = os.getenv("MODEL_ID", "Qwen/Qwen3-Reranker-0.6B")
MODEL_MAX_LENGTH = int(os.getenv("MODEL_MAX_LENGTH", "2048"))
MODEL_BATCH_SIZE = int(os.getenv("MODEL_BATCH_SIZE", "4"))
MODEL_DEVICE_REQUEST = os.getenv("MODEL_DEVICE", "cpu").strip().lower()
MODEL_BACKEND = os.getenv("MODEL_BACKEND", "torch").strip().lower()
MODEL_FILE_NAME = os.getenv("MODEL_FILE_NAME", "").strip()
MODEL_REVISION = os.getenv("MODEL_REVISION", "").strip()
MODEL_TRUST_REMOTE_CODE = os.getenv("MODEL_TRUST_REMOTE_CODE", "0").strip().lower() in {
    "1",
    "true",
    "yes",
}
GPU_REQUIRED = os.getenv("GPU_REQUIRED", "0").strip().lower() in {"1", "true", "yes"}
RETRIEVAL_INSTRUCTION = (
    "Given an agricultural research query, retrieve passages that answer it "
    "precisely. Prioritize exact matches for years, locations, projects, crop "
    "varieties, treatments and replicates, numeric units, statistical scope, "
    "and evidence provenance."
)

if MODEL_DEVICE_REQUEST == "auto":
    MODEL_DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
elif MODEL_DEVICE_REQUEST.startswith("cuda"):
    if not torch.cuda.is_available():
        if GPU_REQUIRED:
            raise RuntimeError("CUDA was requested but is not available")
        MODEL_DEVICE = "cpu"
    else:
        MODEL_DEVICE = MODEL_DEVICE_REQUEST
else:
    MODEL_DEVICE = "cpu"

if MODEL_DEVICE == "cpu":
    torch.set_num_threads(max(1, min(16, os.cpu_count() or 1)))

if MODEL_BACKEND not in {"torch", "onnx"}:
    raise RuntimeError(f"unsupported model backend: {MODEL_BACKEND}")

app = FastAPI(title="WeKnora Adaptive Reranker", version="1.1")
model: CrossEncoder | None = None
model_loaded_at = 0.0
inference_gate = asyncio.Semaphore(1)


class RerankRequest(BaseModel):
    model: str | None = None
    query: str = Field(min_length=1)
    documents: list[Any] = Field(min_length=1)


def normalize_document(document: Any) -> str:
    if isinstance(document, str):
        return document
    if isinstance(document, dict) and isinstance(document.get("text"), str):
        return document["text"]
    raise ValueError("each document must be a string or an object with a text field")


def load_model() -> None:
    global model, model_loaded_at
    model_options: dict[str, Any] = {
        "device": MODEL_DEVICE,
        "max_length": MODEL_MAX_LENGTH,
        "backend": MODEL_BACKEND,
        "trust_remote_code": MODEL_TRUST_REMOTE_CODE,
    }
    if MODEL_REVISION:
        model_options["revision"] = MODEL_REVISION
    if MODEL_BACKEND == "onnx":
        model_options["model_kwargs"] = {
            "provider": "CPUExecutionProvider",
            **({"file_name": MODEL_FILE_NAME} if MODEL_FILE_NAME else {}),
        }
    if "qwen3-reranker" in MODEL_ID.lower():
        model_options.update(
            prompts={"agri-retrieval": RETRIEVAL_INSTRUCTION},
            default_prompt_name="agri-retrieval",
        )
    model = CrossEncoder(MODEL_ID, **model_options)
    model_loaded_at = time.time()


@app.on_event("startup")
def startup() -> None:
    load_model()


@app.get("/health")
def health() -> dict[str, Any]:
    if model is None:
        raise HTTPException(status_code=503, detail="model is loading")
    return {
        "status": "ok",
        "model": MODEL_ID,
        "device": MODEL_DEVICE,
        "backend": MODEL_BACKEND,
        "model_file": MODEL_FILE_NAME or None,
        "revision": MODEL_REVISION or None,
        "max_length": MODEL_MAX_LENGTH,
        "loaded_at": model_loaded_at,
    }


def predict(query: str, documents: list[str]) -> list[float]:
    if model is None:
        raise RuntimeError("model is not loaded")
    pairs = [(query, document) for document in documents]
    scores = model.predict(
        pairs,
        batch_size=MODEL_BATCH_SIZE,
        activation_fn=torch.nn.Sigmoid(),
        show_progress_bar=False,
    )
    return [float(score) for score in scores]


async def rerank_impl(payload: RerankRequest) -> dict[str, Any]:
    try:
        documents = [normalize_document(document) for document in payload.documents]
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    async with inference_gate:
        try:
            scores = await asyncio.to_thread(predict, payload.query, documents)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"rerank failed: {exc}") from exc

    ranked = sorted(enumerate(scores), key=lambda item: item[1], reverse=True)
    return {
        "id": f"rerank-{int(time.time() * 1000)}",
        "model": MODEL_ID,
        "usage": {"total_tokens": 0},
        "results": [
            {
                "index": index,
                "document": documents[index],
                "relevance_score": score,
            }
            for index, score in ranked
        ],
    }


@app.post("/rerank")
async def rerank(payload: RerankRequest) -> dict[str, Any]:
    return await rerank_impl(payload)


@app.post("/v1/rerank")
async def rerank_v1(payload: RerankRequest) -> dict[str, Any]:
    return await rerank_impl(payload)
