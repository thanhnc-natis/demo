import asyncio
import base64
import os
import tempfile
from io import BytesIO
from pathlib import Path
import threading

from fastapi import FastAPI, UploadFile, File, Form, HTTPException

from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict

app = FastAPI(title="Marker PDF Service")

DEFAULT_PREVIEW_LIMIT = int(os.getenv("DOCUMENT_PREVIEW_IMAGE_LIMIT", "6"))
MAX_PREVIEW_LIMIT = int(os.getenv("MARKER_MAX_PREVIEW_IMAGES", "20"))
MARKER_USE_LLM = os.getenv("MARKER_USE_LLM", "0") == "1"

_ARTIFACT_LOCK = threading.Lock()
_MARKER_ARTIFACTS = None
_MARKER_ERROR: Exception | None = None


def _marker_page_index(image_name: str) -> int | None:
    stem = Path(image_name).stem
    parts = [part for part in stem.split("_") if part]
    for idx, token in enumerate(parts):
        if token.lower() == "page" and idx + 1 < len(parts):
            try:
                return int(parts[idx + 1])
            except (TypeError, ValueError):
                continue
    return None


def _clamp_preview_limit(value: int) -> int:
    if value <= 0:
        return DEFAULT_PREVIEW_LIMIT
    return min(value, MAX_PREVIEW_LIMIT)


def _load_artifacts():
    global _MARKER_ARTIFACTS, _MARKER_ERROR
    if _MARKER_ARTIFACTS is not None:
        return _MARKER_ARTIFACTS
    if _MARKER_ERROR:
        raise RuntimeError(_MARKER_ERROR)
    with _ARTIFACT_LOCK:
        if _MARKER_ARTIFACTS is None and _MARKER_ERROR is None:
            try:
                device = os.getenv("MARKER_TORCH_DEVICE")
                kwargs = {}
                if device:
                    kwargs["device"] = device
                _MARKER_ARTIFACTS = create_model_dict(**kwargs)
            except Exception as exc:
                _MARKER_ERROR = exc
                raise RuntimeError(exc)
    return _MARKER_ARTIFACTS


def _encode_preview(idx: int, name: str, pil_image) -> dict:
    try:
        image = pil_image.convert("RGB") if pil_image.mode != "RGB" else pil_image
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    except Exception:
        return {}
    return {
        "id": f"marker_{idx}",
        "name": name,
        "page": _marker_page_index(name),
        "width": getattr(pil_image, "width", 0) or 0,
        "height": getattr(pil_image, "height", 0) or 0,
        "media_type": "image/png",
        "data": encoded,
    }


def _build_converter():
    artifacts = _load_artifacts()
    config: dict[str, object] = {"extract_images": True, "MarkdownRenderer_paginate_output": False}
    if MARKER_USE_LLM:
        config["use_llm"] = True
    return PdfConverter(artifact_dict=artifacts, config=config)


def _render_pdf(pdf_path: Path, preview_limit: int) -> dict:
    converter = _build_converter()
    rendered = converter(str(pdf_path))
    preview_images: list[dict] = []
    for idx, (name, pil_image) in enumerate(rendered.images.items()):
        if idx >= preview_limit:
            break
        encoded = _encode_preview(idx, name, pil_image)
        if encoded:
            preview_images.append(encoded)
    marker_text = (getattr(rendered, "markdown", "") or "").strip()
    marker_metadata = getattr(rendered, "metadata", {}) or {}
    pages = getattr(converter, "page_count", None)
    return {
        "pages": pages,
        "text": marker_text,
        "metadata": marker_metadata,
        "preview_images": preview_images,
    }


async def _save_upload(upload: UploadFile) -> Path:
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    written = 0
    try:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            temp_file.write(chunk)
            written += len(chunk)
    finally:
        temp_file.close()
    if written == 0:
        Path(temp_file.name).unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="Empty file uploaded")
    return Path(temp_file.name)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/convert")
async def convert_pdf(file: UploadFile = File(...), preview_limit: int = Form(DEFAULT_PREVIEW_LIMIT)):
    filename = (file.filename or "").lower()
    if not filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF uploads are supported")
    limit = _clamp_preview_limit(preview_limit)
    pdf_path = await _save_upload(file)
    try:
        result = await asyncio.to_thread(_render_pdf, pdf_path, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=f"Marker initialization failed: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Marker conversion failed: {exc}") from exc
    finally:
        pdf_path.unlink(missing_ok=True)
    return result
