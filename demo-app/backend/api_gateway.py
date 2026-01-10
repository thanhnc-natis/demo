import asyncio
import base64
import json
import os
import hashlib
import re
import uuid
from datetime import datetime
from io import BytesIO
from pathlib import Path
import urllib.parse

import httpx
from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    Depends,
    HTTPException,
    Query,
    BackgroundTasks,
    UploadFile,
    File,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy.orm import Session
from PIL import Image

from chat_handler import answer_question
from database import Base, engine, get_db, SessionLocal
from ingestion_queue import IngestionQueue
from kg_worker import KGWorker
from models import AnalysisLog, DocumentRecord
from PyPDF2 import PdfReader
from neo4j_client import Neo4jClient
from openai import OpenAI
import feedparser

# Optional fixed translator (no-LM) for Khmer -> Vietnamese
try:
    from deep_translator import GoogleTranslator
except Exception:
    GoogleTranslator = None

# Create DB tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Media Monitor API Gateway")

origins = [
    "http://10.100.21.122:5173",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
kg_worker = KGWorker()
try:
    neo4j_client = Neo4jClient.from_env()
    try:
        neo4j_client.ensure_constraints()
        print("Neo4j constraints ensured.")
    except Exception as exc:
        print(f"Neo4j constraint setup skipped: {exc}")
        neo4j_client.close()
        neo4j_client = None
    if neo4j_client:
        print("Neo4j client initialized.")
except Exception as exc:
    neo4j_client = None
    print(f"Neo4j client not available: {exc}")

ingestion_queue = IngestionQueue.from_env()
PERSON_SERVICE_URL = os.getenv("PERSON_SERVICE_URL", "http://person-service:8001")
DOCUMENT_STORE = Path(os.getenv("DOCUMENT_STORE", "./data/documents"))
DOCUMENT_STORE.mkdir(parents=True, exist_ok=True)
DOCUMENT_PREVIEW_IMAGE_LIMIT = int(os.getenv("DOCUMENT_PREVIEW_IMAGE_LIMIT", "6"))
MARKER_SERVICE_URL = (os.getenv("MARKER_SERVICE_URL") or "").rstrip("/")
MARKER_SERVICE_TIMEOUT = float(os.getenv("MARKER_SERVICE_TIMEOUT", "120"))

def _write_social_graph_to_neo4j(graph: dict):
    """Persist a simple person->account->post graph into Neo4j."""
    if not neo4j_client or not graph:
        return
    person_name = graph.get("person_name") or graph.get("name")
    person_id = graph.get("person_id") or graph.get("id") or person_name
    accounts = graph.get("accounts") or []
    posts = graph.get("posts") or []
    interactions = graph.get("interactions") or []
    edges = graph.get("edges") or []
    primary_account_id = graph.get("primary_account_id") or (accounts[0]["id"] if accounts else None)
    # If graph is mock, drop the first account (assumed to belong to the person) to avoid Person->Account edges
    if graph.get("mock") and accounts:
        accounts = accounts[1:]
    # If interactions missing but edges provided, map mock edges (commented_on) to interactions
    if not interactions and edges:
        post_ids = {p.get("id") for p in posts}
        acc_lookup = {a.get("id"): a for a in accounts}
        for e in edges:
            if (e.get("type") or "").lower() != "commented_on":
                continue
            src = e.get("source")
            tgt = e.get("target")
            if src in (None, "person"):
                continue
            if tgt not in post_ids:
                continue
            acc = acc_lookup.get(src) or {}
            interactions.append(
                {
                    "account_id": src,
                    "post_id": tgt,
                    "type": "comment",
                    "handle": acc.get("handle"),
                    "display_name": acc.get("display_name"),
                }
            )
    # If interactions are missing, synthesize commenters (non-primary accounts) on posts
    if not interactions and posts:
        commenter_accounts = [a for a in accounts if a.get("id") != primary_account_id] or accounts
        for idx, post in enumerate(posts):
            acc = commenter_accounts[idx % len(commenter_accounts)]
            interactions.append(
                {
                    "account_id": acc.get("id"),
                    "post_id": post.get("id"),
                    "type": "comment",
                    "handle": acc.get("handle"),
                    "display_name": acc.get("display_name"),
                }
            )
    if not person_name:
        return
    cypher = """
    // create person and accounts (no Person->Account edges)
    MERGE (p:Person {person_id: $person_id})
    ON CREATE SET p.name = $person_name
    ON MATCH SET p.name = coalesce(p.name, $person_name)
    // remove legacy HAS_ACCOUNT edges for this person
    WITH p
    OPTIONAL MATCH (p)-[ha:HAS_ACCOUNT]->(:Account)
    DELETE ha
    WITH p, $accounts AS accounts, $posts AS posts
    UNWIND accounts AS acc
      MERGE (a:Account {id: acc.id})
      SET a.handle = acc.handle,
          a.display_name = coalesce(acc.display_name, acc.handle),
          a.name = coalesce(acc.display_name, acc.handle, acc.id)
    WITH p, accounts, posts, $interactions AS interactions
    UNWIND posts AS post
      MERGE (po:Post {id: post.id})
      SET po.text = post.text,
          po.timestamp = post.timestamp,
          po.summary = post.summary,
          po.caption = coalesce(post.caption, 'X Post'),
          po.source = coalesce(post.source, 'X')
      MERGE (p)-[:POSTED]->(po)
    WITH p, interactions
    UNWIND interactions AS inter
      MATCH (po:Post {id: inter.post_id})
      MERGE (c:Account {id: inter.account_id})
      SET c.handle = coalesce(inter.handle, c.handle),
          c.display_name = coalesce(inter.display_name, c.display_name, c.handle),
          c.name = coalesce(c.display_name, c.handle, c.id)
      MERGE (c)-[:COMMENTED_ON {type: coalesce(inter.type, 'comment')}]->(po)
    """
    try:
        with neo4j_client.session() as session:
            session.run(
                cypher,
                person_name=person_name,
                person_id=person_id,
                accounts=accounts,
                posts=posts,
                interactions=interactions,
            )
    except Exception as exc:
        print(f"[KG] Failed to write social graph: {exc}")

def _social_graph_for_name(name: str) -> dict:
    """Best-effort account lookup for a name; falls back to a mock account+posts."""
    clean = (name or "").strip()
    token = os.getenv("X_BEARER_TOKEN")
    account = None
    if token and clean:
        try:
            url = f"https://api.twitter.com/2/users/by?usernames={urllib.parse.quote(clean)}"
            req = httpx.Request("GET", url, headers={"Authorization": f"Bearer {token}"})
            with httpx.Client(timeout=5.0) as client:
                resp = client.send(req)
            if resp.status_code == 200:
                data = resp.json().get("data") or []
                if data:
                    u = data[0]
                    account = {
                        "id": u.get("id") or clean,
                        "handle": f"@{u.get('username')}" if u.get("username") else None,
                        "display_name": u.get("name") or u.get("username") or clean,
                    }
        except Exception:
            account = None
    if not account:
        slug = clean.lower().replace(" ", "_") or "unknown"
        account = {"id": f"acct_{slug}", "handle": f"@{slug}", "display_name": clean or slug}
    posts = [
        {"id": f"{account['id']}_p1", "account_id": account["id"], "text": f"Latest update about {clean}", "timestamp": datetime.utcnow().isoformat(), "source": "X", "caption": "X Post"},
        {"id": f"{account['id']}_p2", "account_id": account["id"], "text": f"Another note on {clean}", "timestamp": datetime.utcnow().isoformat(), "source": "X", "caption": "X Post"},
    ]
    # mock interactions from other accounts
    commenter1 = {"id": f"{account['id']}_c1", "handle": f"@friend_of_{account['id']}", "display_name": "Top commenter"}
    commenter2 = {"id": f"{account['id']}_c2", "handle": f"@fan_of_{account['id']}", "display_name": "Fan"}
    interactions = [
        {"account_id": commenter1["id"], "post_id": posts[0]["id"], "type": "comment", "handle": commenter1["handle"], "display_name": commenter1["display_name"]},
        {"account_id": commenter2["id"], "post_id": posts[1]["id"], "type": "comment", "handle": commenter2["handle"], "display_name": commenter2["display_name"]},
    ]
    accounts_extra = [commenter1, commenter2]
    accounts_full = [account] + accounts_extra
    return {
        "source": "x",
        "person_name": clean,
        "person_id": clean,
        "accounts": accounts_full,
        "primary_account_id": account["id"],
        "posts": posts,
        "edges": [],
        "interactions": interactions,
        "mock": True,
    }


def _format_graph_node(node):
    """Convert a Neo4j node into a serializable dict."""
    if node is None:
        return None
    props = dict(node)
    labels = list(getattr(node, "labels", []))
    element_id = getattr(node, "element_id", None)
    display_name = (
        props.get("name")
        or props.get("display_name")
        or props.get("handle")
        or props.get("title")
        or props.get("caption")
        or props.get("id")
        or props.get("person_id")
        or element_id
    )
    node_type = props.get("type") or (labels[0] if labels else "Node")
    preferred_id = (
        props.get("person_id")
        or props.get("id")
        or element_id
    )
    return {
        "id": preferred_id,
        "element_id": element_id,
        "labels": labels,
        "type": node_type,
        "display_name": display_name,
        "properties": props,
    }


def _get_person_graph(identifier: str):
    """Fetch a person node and its level 1 + level 2 neighbors."""
    if not identifier:
        raise HTTPException(status_code=400, detail="identifier is required")
    if not neo4j_client:
        raise HTTPException(status_code=503, detail="Neo4j not configured")
    person_query = """
    MATCH (p:Person)
    WHERE elementId(p) = $identifier
       OR p.person_id = $identifier
       OR toLower(p.name) = toLower($identifier)
    RETURN p
    LIMIT 1
    """
    level1_query = """
    MATCH (p:Person)
    WHERE elementId(p) = $identifier
       OR p.person_id = $identifier
       OR toLower(p.name) = toLower($identifier)
    MATCH (p)-[r]-(n)
    RETURN DISTINCT n AS node,
           type(r) AS rel_type,
           CASE WHEN startNode(r) = p THEN 'out' ELSE 'in' END AS direction,
           elementId(r) AS rel_id
    LIMIT 80
    """
    level2_query = """
    MATCH (p:Person)
    WHERE elementId(p) = $identifier
       OR p.person_id = $identifier
       OR toLower(p.name) = toLower($identifier)
    MATCH (p)-[r1]-(n1)-[r2]-(n2)
    WHERE n2 <> p
    RETURN DISTINCT n1 AS via_node,
           n2 AS node,
           type(r2) AS rel_type,
           CASE WHEN startNode(r2) = n1 THEN 'out' ELSE 'in' END AS direction,
           elementId(r2) AS rel_id
    LIMIT 150
    """
    with neo4j_client.session() as session:
        person_record = session.run(person_query, identifier=identifier).single()
        if not person_record:
            raise HTTPException(status_code=404, detail="Person node not found")
        person_node = _format_graph_node(person_record["p"])

        level1_records = session.run(level1_query, identifier=identifier).data()
        level1_nodes = []
        for rec in level1_records:
            formatted = _format_graph_node(rec.get("node"))
            if not formatted:
                continue
            level1_nodes.append(
                {
                    "node": formatted,
                    "relationship": rec.get("rel_type"),
                    "direction": rec.get("direction"),
                    "relationship_id": rec.get("rel_id"),
                }
            )

        level2_records = session.run(level2_query, identifier=identifier).data()
        level2_nodes = []
        for rec in level2_records:
            node_formatted = _format_graph_node(rec.get("node"))
            via_formatted = _format_graph_node(rec.get("via_node"))
            if not node_formatted or not via_formatted:
                continue
            level2_nodes.append(
                {
                    "node": node_formatted,
                    "via": {
                        "id": via_formatted.get("id"),
                        "element_id": via_formatted.get("element_id"),
                        "display_name": via_formatted.get("display_name"),
                        "type": via_formatted.get("type"),
                    },
                    "relationship": rec.get("rel_type"),
                    "direction": rec.get("direction"),
                    "relationship_id": rec.get("rel_id"),
                }
            )

    return person_node, level1_nodes, level2_nodes


def _build_graph_snapshot(person, level1_nodes, level2_nodes, person_profile=None):
    """Compose condensed text context for the LLM."""
    def _condense_entry(entry):
        node = entry.get("node") or {}
        props = node.get("properties") or {}
        highlights = {}
        for key in ("role", "title", "summary", "description", "handle", "source", "sentiment", "keywords"):
            val = props.get(key)
            if val:
                highlights[key] = val
        condensed = {
            "name": node.get("display_name"),
            "type": node.get("type"),
            "labels": node.get("labels"),
            "relationship": entry.get("relationship"),
            "direction": entry.get("direction"),
            "highlights": highlights,
        }
        via = entry.get("via")
        if via:
            condensed["via"] = via
        return condensed

    snapshot = {
        "person": {
            "name": person.get("display_name"),
            "type": person.get("type"),
            "labels": person.get("labels"),
            "properties": person.get("properties"),
        },
        "level1": [_condense_entry(e) for e in level1_nodes[:20]],
        "level2": [_condense_entry(e) for e in level2_nodes[:30]],
    }
    if person_profile:
        snapshot["person"]["profile"] = person_profile
    return json.dumps(snapshot, ensure_ascii=False)


def _generate_fallback_report(person, level1_nodes, level2_nodes):
    """Return a simple Vietnamese report if LLM is unavailable."""
    name = person.get("display_name") or person.get("properties", {}).get("name") or "Muc tieu"
    lvl1_names = ", ".join({(n.get("node") or {}).get("display_name") for n in level1_nodes if (n.get("node") or {}).get("display_name")}) or "khong ro"
    lvl2_names = ", ".join({(n.get("node") or {}).get("display_name") for n in level2_nodes if (n.get("node") or {}).get("display_name")}) or "khong ro"
    return {
        "tom_tat_hanh_dong": f"Chưa có dữ liệu chi tiết cho {name}. Chỉ nhìn thấy kết nối cấp 1: {lvl1_names}. Cấp 2: {lvl2_names}.",
        "ho_so_doi_tuong": f"{name} là nhân vật được đánh dấu trong đồ thị, nhưng hệ thống chưa có hồ sơ đầy đủ.",
        "danh_gia_tac_dong": "Chưa đủ căn cứ để đánh giá tác động, cần bổ sung dữ liệu hoạt động gần đây.",
        "khuyen_nghi": "Thu thập thêm nội dung, xác minh các mối quan hệ quan trọng và theo dõi dòng thảo luận thời gian thực.",
    }


def _fetch_person_profile(person):
    """Retrieve authoritative person profile from person-service."""
    props = person.get("properties") or {}
    identifier = (
        props.get("person_id")
        or props.get("id")
        or person.get("id")
        or props.get("name")
        or person.get("display_name")
    )
    if not identifier:
        return None
    base = (PERSON_SERVICE_URL or "").rstrip("/")
    if not base:
        return None
    url = f"{base}/people/{urllib.parse.quote(identifier)}"
    try:
        with httpx.Client(timeout=6.0) as client:
            resp = client.get(url)
        if resp.status_code == 200:
            return resp.json()
    except Exception as exc:
        print(f"[PersonProfile] lookup failed for {identifier}: {exc}")
    return None


def _compose_target_profile(profile: dict | None) -> str | None:
    if not profile:
        return None
    lines = []
    name = profile.get("name")
    if name:
        lines.append(f"- Họ tên: {name}")
    nationality = profile.get("nationality")
    if nationality:
        lines.append(f"- Quốc tịch: {nationality}")
    dob = profile.get("date_of_birth")
    if dob:
        lines.append(f"- Ngày sinh: {dob}")
    pid = profile.get("id_number")
    if pid:
        lines.append(f"- Định danh: {pid}")
    aliases = profile.get("aliases")
    if aliases:
        lines.append(f"- Bí danh: {', '.join(aliases)}")
    note = profile.get("note")
    if note:
        lines.append(f"- Ghi chú: {note}")
    if not lines:
        return None
    return "\n".join(lines)


def _apply_person_profile(report: dict, person_profile: dict | None) -> dict:
    if not isinstance(report, dict):
        report = {}
    profile_text = _compose_target_profile(person_profile)
    if profile_text:
        report = dict(report)
        report["ho_so_doi_tuong"] = profile_text
    return report


def _format_report_section(text: str) -> str:
    """Return 1 short paragraph plus concise bullets."""
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return ""
    paragraph = lines[0].lstrip("-• ").strip()
    bullets = []
    for raw in lines[1:]:
        cleaned = raw.lstrip("-• ").strip()
        if cleaned:
            bullets.append(f"- {cleaned}")
    if not paragraph and bullets:
        paragraph = bullets.pop(0).lstrip("- ").strip()
    parts = [paragraph] if paragraph else []
    parts.extend(bullets)
    return "\n".join(parts)


def _to_plain_paragraph(text: str, limit: int = 800) -> str:
    """Collapse whitespace into a single readable paragraph."""
    if not text:
        return ""
    normalized = re.sub(r"\s+", " ", text).strip()
    if limit:
        normalized = normalized[:limit].rstrip()
    return normalized


def _parse_json_response(raw_text: str) -> dict:
    cleaned = (raw_text or "").strip()
    if not cleaned:
        return {}
    fence_prefixes = ("```json", "```JSON", "```")
    for prefix in fence_prefixes:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :].strip()
            break
    if cleaned.endswith("```"):
        cleaned = cleaned[: -3].strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail="LLM returned invalid JSON")


def _load_mock_person_report(person: dict) -> dict | None:
    mock_path = Path(__file__).parent / "mock_person_report_le_trung_khoa.json"
    if not mock_path.exists():
        return None
    try:
        payload = json.loads(mock_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    target_names = {
        (payload.get("person") or {}).get("display_name"),
        ((payload.get("person") or {}).get("properties") or {}).get("name"),
        payload.get("person_name"),
    }
    target_ids = {
        (payload.get("person") or {}).get("id"),
        ((payload.get("person") or {}).get("properties") or {}).get("person_id"),
    }
    current_name = (
        person.get("display_name")
        or (person.get("properties") or {}).get("name")
        or person.get("id")
        or ""
    ).strip().lower()
    current_id = (
        (person.get("properties") or {}).get("person_id")
        or person.get("id")
        or ""
    ).strip().lower()
    normalized_targets = { (name or "").strip().lower() for name in target_names if name }
    normalized_ids = { (pid or "").strip().lower() for pid in target_ids if pid }
    if (current_name and current_name in normalized_targets) or (current_id and current_id in normalized_ids):
        return payload
    return None


def _build_document_kg_prompt(summary_text: str, metadata: dict | None) -> str:
    metadata_json = json.dumps(metadata or {}, ensure_ascii=False, indent=2)
    return (
        "Bạn là hệ thống trích xuất tri thức từ báo cáo tiếng Việt.\n"
        "Sử dụng phần tóm tắt dưới đây để xác định các thực thể (entities) quan trọng "
        "và các quan hệ (relations) giữa chúng.\n\n"
        "Trả về JSON với cấu trúc:\n"
        '{\n'
        '  "entities": [\n'
        '    {"name": "Tên", "type": "Person|Organization|Location|Event|Other", "summary": "Mô tả ngắn"}\n'
        "  ],\n"
        '  "relations": [\n'
        '    {"source": "Tên nguồn", "target": "Tên đích", "type": "Quan hệ", "evidence": "Câu chứng minh"}\n'
        "  ]\n"
        "}\n\n"
        "Yêu cầu:\n"
        "- Chỉ dựa trên nội dung tóm tắt; không bịa thông tin.\n"
        "- Ưu tiên thực thể có vai trò chính và quan hệ rõ ràng.\n"
        "- Nếu không chắc chắn, để mảng rỗng.\n"
        "- Chỉ trả JSON thuần hợp lệ.\n\n"
        f"Tóm tắt tài liệu:\n\"\"\"\n{summary_text.strip()}\n\"\"\"\n\n"
        f"Metadata bổ sung (nếu có):\n{metadata_json}\n"
    )


async def _extract_entities_from_summary(summary_text: str, metadata: dict | None, client: OpenAI) -> dict:
    prompt = _build_document_kg_prompt(summary_text, metadata)
    response = await asyncio.to_thread(
        client.chat.completions.create,
        model=os.getenv("DOCUMENT_KG_MODEL", "llama-3.1-8b-instant"),
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=600,
    )
    raw_text = (response.choices[0].message.content or "").strip()
    data = _parse_json_response(raw_text) or {}
    data.setdefault("entities", [])
    data.setdefault("relations", [])
    # Normalize naming to keep compatibility with front-end expectations
    if "relationships" in data and not data.get("relations"):
        data["relations"] = data["relationships"]
    if "events" not in data:
        data["events"] = []
    return data


def _extract_pdf_images(pdf_path: Path, max_images: int = 10, reader: PdfReader | None = None) -> list[dict]:
    results: list[dict] = []
    try:
        local_reader = reader or PdfReader(str(pdf_path))
    except Exception as exc:
        print(f"[Documents] Failed to open {pdf_path} for image extraction: {exc}")
        return results
    for page_index, page in enumerate(local_reader.pages):
        if len(results) >= max_images:
            break
        resources = page.get("/Resources")
        if not resources:
            continue
        xobject = resources.get("/XObject")
        if not xobject:
            continue
        try:
            xobject = xobject.get_object()
        except Exception:
            continue
        for _, obj in xobject.items():
            if len(results) >= max_images:
                break
            try:
                subtype = obj.get("/Subtype")
            except Exception:
                continue
            if subtype != "/Image":
                continue
            try:
                base_data = obj.get_data()
            except Exception:
                continue
            width = obj.get("/Width") or 0
            height = obj.get("/Height") or 0
            color_space = obj.get("/ColorSpace")
            if isinstance(color_space, list) and color_space:
                color_space = color_space[0]
            filter_name = obj.get("/Filter")
            media_type = "image/png"
            image_bytes = None
            if filter_name == "/DCTDecode":
                image_bytes = base_data
                media_type = "image/jpeg"
            elif filter_name == "/JPXDecode":
                image_bytes = base_data
                media_type = "image/jp2"
            else:
                mode = "RGB"
                if color_space == "/DeviceCMYK":
                    mode = "CMYK"
                elif color_space == "/DeviceGray":
                    mode = "L"
                try:
                    img = Image.frombytes(mode, (width, height), base_data)
                    if mode == "CMYK":
                        img = img.convert("RGB")
                    buffer = BytesIO()
                    img.save(buffer, format="PNG")
                    image_bytes = buffer.getvalue()
                    media_type = "image/png"
                except Exception:
                    continue
            if not image_bytes:
                continue
            results.append(
                {
                    "id": f"page{page_index}_{len(results)}",
                    "page": page_index,
                    "width": width,
                    "height": height,
                    "media_type": media_type,
                    "data": base64.b64encode(image_bytes).decode("ascii"),
                }
            )
    return results


def _convert_pdf_with_marker(file_path: Path, preview_limit: int = DOCUMENT_PREVIEW_IMAGE_LIMIT) -> dict | None:
    if not MARKER_SERVICE_URL:
        return None
    endpoint = urllib.parse.urljoin(MARKER_SERVICE_URL + "/", "convert")
    data = {"preview_limit": str(preview_limit)}
    try:
        with file_path.open("rb") as fh:
            response = httpx.post(
                endpoint,
                data=data,
                files={"file": (file_path.name, fh, "application/pdf")},
                timeout=MARKER_SERVICE_TIMEOUT,
            )
    except Exception as exc:
        print(f"[Documents] marker service unreachable: {exc}")
        return None
    if response.status_code >= 400:
        print(f"[Documents] marker service error {response.status_code}: {response.text[:200]}")
        return None
    try:
        payload = response.json()
    except ValueError:
        print("[Documents] marker service returned invalid JSON")
        return None
    return {
        "pages": payload.get("pages"),
        "text": payload.get("text") or "",
        "metadata": payload.get("metadata") or {},
        "preview_images": payload.get("preview_images") or [],
    }


def _audit_llm_call(model_name, prompt_preview, success, person_identifier=None, response_preview=None, error_message=None, section=None):
    """Simple console audit for LLM invocations."""
    prompt_snippet = (prompt_preview or "").strip()
    response_snippet = (response_preview or "").strip()
    error_snippet = (error_message or "").strip()
    log = {
        "model": model_name,
        "person_id": person_identifier,
        "success": bool(success),
        "section": section,
        "prompt_preview": prompt_snippet[:500],
        "response_preview": response_snippet[:500],
        "error": error_snippet[:300] if error_snippet else None,
    }
    print("[LLM Audit]", json.dumps(log, ensure_ascii=False))


def _generate_graph_report(person, level1_nodes, level2_nodes, person_profile=None):
    """Use Groq/OpenAI to generate a Vietnamese analysis report."""
    fallback_report = _generate_fallback_report(person, level1_nodes, level2_nodes)
    api_key = os.getenv("GROQ_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return _apply_person_profile(fallback_report, person_profile)
    client = OpenAI(
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.groq.com/openai/v1"),
    )
    snapshot = _build_graph_snapshot(person, level1_nodes, level2_nodes, person_profile=person_profile)
    model_name = os.getenv("GRAPH_REPORT_MODEL", "llama-3.1-8b-instant")
    person_identifier = (
        (person.get("properties") or {}).get("person_id")
        or (person.get("properties") or {}).get("id")
        or person.get("id")
    )
    system_prompt = (
        "Bạn là giám sát tình báo thuộc chính phủ Việt Nam. Luôn đánh giá hoạt động mạng xã hội bằng tiếng Việt có dấu, "
        "giọng nghiêm túc, ưu tiên góc nhìn an ninh nhà nước và nhấn mạnh nguy cơ ảnh hưởng chính trị."
    )
    section_instructions = {
        "tom_tat_hanh_dong": "Tóm tắt các hành động, bài đăng, thông điệp và tương tác gần nhất của đối tượng và nguồn liên quan.",
        "danh_gia_tac_dong": "Đánh giá tác động xã hội/chính trị/kinh tế của các hành động và kết nối đó.",
        "khuyen_nghi": "Đưa ra khuyến nghị chiến lược cụ thể cho 24-72 giờ tới để xử lý hoặc chủ động với tình huống.",
    }

    def _request_section(section_key: str, directive: str) -> str | None:
        prompt = (
            f"Nhi?m v?: {directive}\n"
            "H??ng d?n:\n"
            "- Lu?n s? d?ng m?i d? li?u hi?n c?; n?u thi?u v?n ph?i r?t ra nh?n ??nh an to?n v? n?u r? ph?n thi?u.\n"
            "- Vi?t 1 ?o?n v?n ng?n 1-2 c?u tr??c (kh?ng b?t ??u b?ng '-').\n"
            "- Sau ?? cung c?p t?i ?a 4 g?ch ??u d?ng, m?i d?ng b?t ??u b?ng '- '.\n"
            "- Kh?ng th?m ti?u ??, kh?ng markdown kh?c, kh?ng ?? tr?ng n?i dung.\n"
            f"D? li?u ??u v?o (JSON):\n```json\n{snapshot}\n```"
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
        prompt_for_audit = f"{directive}\n{snapshot}"
        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=messages,
                temperature=0.2,
                max_tokens=400,
            )
        except Exception as exc:
            _audit_llm_call(
                model_name,
                prompt_for_audit,
                success=False,
                person_identifier=person_identifier,
                error_message=str(exc),
                section=section_key,
            )
            return None
        choice = (response.choices or [{}])[0]
        message = getattr(choice, 'message', None)
        if isinstance(message, dict):
            content = message.get('content')
        else:
            content = getattr(message, 'content', None)
        if not content:
            _audit_llm_call(
                model_name,
                prompt_for_audit,
                success=False,
                person_identifier=person_identifier,
                error_message='Empty content',
                section=section_key,
            )
            return None
        formatted = _format_report_section(content)
        _audit_llm_call(
            model_name,
            prompt_for_audit,
            success=True,
            person_identifier=person_identifier,
            response_preview=formatted[:500],
            section=section_key,
        )
        return formatted

    report = dict(fallback_report)
    for key, directive in section_instructions.items():
        value = _request_section(key, directive)
        if value:
            report[key] = value

    return _apply_person_profile(report, person_profile)



def _extract_pdf_metadata(file_path: Path) -> dict:
    pages = 0
    metadata = {}
    pdf_text_parts: list[str] = []
    preview_images: list[dict] = []
    marker_metadata: dict = {}
    try:
        reader = PdfReader(str(file_path))
        pages = len(reader.pages)
        metadata = {k: str(v) for k, v in (reader.metadata or {}).items()}
        for page in reader.pages:
            try:
                pdf_text_parts.append(page.extract_text() or "")
            except Exception:
                pdf_text_parts.append("")
        try:
            preview_images = _extract_pdf_images(
                file_path, max_images=DOCUMENT_PREVIEW_IMAGE_LIMIT, reader=reader
            )
        except Exception:
            preview_images = []
    except Exception as exc:
        print(f"[Documents] Failed to parse PDF {file_path}: {exc}")
    marker_details = _convert_pdf_with_marker(
        file_path, preview_limit=DOCUMENT_PREVIEW_IMAGE_LIMIT
    )
    marker_text = ""
    marker_metadata: dict = {}
    if marker_details:
        marker_text = marker_details.get("text") or ""
        marker_metadata = marker_details.get("metadata") or {}
        preview_images = marker_details.get("preview_images") or preview_images
        marker_pages = marker_details.get("pages")
        if marker_pages:
            pages = marker_pages
    pdf_text = "\n".join(pdf_text_parts).strip()
    if marker_text and pdf_text:
        full_text = marker_text if len(marker_text) >= len(pdf_text) else pdf_text
    else:
        full_text = marker_text or pdf_text
    ocr_text = marker_text or ""
    if not full_text:
        full_text = pdf_text
    if not preview_images:
        try:
            preview_images = _extract_pdf_images(
                file_path, max_images=DOCUMENT_PREVIEW_IMAGE_LIMIT
            )
        except Exception:
            preview_images = []
    excerpt = full_text[:10000]
    text_source = "marker" if marker_text else ("pdf" if pdf_text else "unknown")
    return {
        "pages": pages,
        "full_text": full_text,
        "excerpt": excerpt,
        "metadata": metadata,
        "ocr_text": ocr_text,
        "preview_images": preview_images,
        "marker_metadata": marker_metadata,
        "text_source": text_source,
    }


async def _summarize_document_text(content: str) -> str:
    cleaned = (content or "").strip()
    if not cleaned:
        return ""
    api_key = os.getenv("GROQ_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return _to_plain_paragraph(cleaned[:800])
    client = OpenAI(
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.groq.com/openai/v1"),
    )
    prompt = (
        "Tóm tắt tài liệu sau thành một đoạn văn tiếng Việt ngắn gọn "
        "không gạch đầu dòng, không markdown, tối đa 3 câu, chỉ văn bản thường.\n"
        f"Nội dung:\n{cleaned[:6000]}"
    )
    try:
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model=os.getenv("DOCUMENT_SUMMARY_MODEL", "llama-3.1-8b-instant"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=400,
        )
        text_resp = (response.choices[0].message.content or "").strip()
        return _to_plain_paragraph(text_resp) if text_resp else _to_plain_paragraph(cleaned[:800])
    except Exception as exc:
        print(f"[Documents] Summary generation failed: {exc}")
        return _to_plain_paragraph(cleaned[:800])


def _document_to_dict(rec: DocumentRecord) -> dict:
    return {
        "id": rec.id,
        "filename": rec.filename,
        "original_name": rec.original_name,
        "pages": rec.pages,
        "summary": rec.summary,
        "text_excerpt": rec.text_excerpt,
        "metadata": rec.metadata_json or {},
        "created_at": rec.created_at.isoformat() if rec.created_at else None,
    }


def _translate_to_vi(text: str, client: OpenAI | None = None) -> str:
    """Best-effort Vietnamese translation; returns original text on failure."""
    content = (text or "").strip()
    if not content:
        return ""

    def _contains_khmer(val: str) -> bool:
        return any("\u1780" <= ch <= "\u17ff" for ch in val)

    # Prefer fixed translator if Khmer detected and library is available
    if _contains_khmer(content) and GoogleTranslator:
        try:
            fixed = GoogleTranslator(source="km", target="vi").translate(content)
            if fixed:
                return fixed.strip()
        except Exception:
            pass

    if not client:
        api_key = os.getenv("GROQ_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            return ""
        client = OpenAI(
            api_key=api_key,
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.groq.com/openai/v1"),
        )
    prompt = f"Translate to Vietnamese, keep concise, no markdown:\n{content[:1500]}"
    khmer_prompt = f"Translate this Khmer text to Vietnamese. Return Vietnamese only, no markdown:\n{content[:1500]}"
    try:
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=400,
        )
        result = (resp.choices[0].message.content or "").strip()
        if result:
            return result
        # Retry with explicit Khmer->VI instruction if text contains Khmer script
        if _contains_khmer(content):
            resp = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "user", "content": khmer_prompt}],
                temperature=0.2,
                max_tokens=400,
            )
            fallback = (resp.choices[0].message.content or "").strip()
            if fallback:
                return fallback
        return ""
    except Exception:
        return ""


class ChatRequest(BaseModel):
    question: str


class CrawlRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    keyword: str
    limit: int = 5
    source_type: str = "news"  # news | social | both
    country: str | None = None  # optional ISO country filter for newsdata/newsapi
    source_id: str | None = None  # optional specific source filter
    lang: str | None = Field(default=None, alias="language")
    source: str | None = None  # alias for source_id
    sources: list[str] | None = None  # optional list of source ids


class LiveToggleRequest(BaseModel):
    enabled: bool


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        try:
            self.active_connections.remove(websocket)
        except ValueError:
            pass

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                try:
                    self.active_connections.remove(connection)
                except ValueError:
                    pass


manager = ConnectionManager()
_stop_event = asyncio.Event()
consumer_task: asyncio.Task | None = None
kg_worker_task: asyncio.Task | None = None
processing_enabled = False


def _get_audio_client():
    api_key = os.getenv("GROQ_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    return OpenAI(
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.groq.com/openai/v1"),
    )


def _background_upsert_crawled_news(articles, search_terms=None):
    """Push crawled articles into Neo4j after response returns."""
    if not neo4j_client:
        return
    search_terms = [t.strip() for t in (search_terms or []) if (t or "").strip()]

    def _news_id_from_item(item: dict) -> int:
        base_str = (
            item.get("source")
            or item.get("url")
            or item.get("title")
            or str(datetime.now().timestamp())
        )
        raw = hashlib.md5(base_str.encode("utf-8")).digest()[:8]
        hashed = int.from_bytes(raw, byteorder="big", signed=False) % (2**63 - 1)
        return hashed or 1

    for item in articles:
        news_hash = _news_id_from_item(item)
        try:
            neo4j_client.upsert_kg_item(
                news_id=news_hash,
                kg_data={"entities": [], "events": [], "relations": []},
                source=item.get("source") or "NewsAPI",
                summary=item.get("title") or item.get("description") or "",
                translation_vi=item.get("vietnamese_translation") or "",
                timestamp=item.get("published_at"),
                keywords=search_terms,
            )
        except Exception as exc:
            print(
                "Neo4j upsert error for crawled news",
                {
                    "error": str(exc),
                    "news_id": news_hash,
                    "title": item.get("title"),
                    "source": item.get("source"),
                    "url": item.get("url"),
                },
    )


DEFAULT_RSS_FEEDS = [
    "https://kohsantepheapdaily.com.kh/feed",
    "https://www.kampucheathmey.com/feed",
    "https://cen.com.kh/feed",
]


async def process_callback(data):
    """Transform ingestion data for frontend WebSocket"""

    summary_text = (
        data.get("summary")
        or data.get("english_summary")
        or data.get("text")
        or data.get("headline_ocr")
        or ""
    )
    translation_text = (
        data.get("vietnamese_translation")
        or data.get("subtitle_vi")
        or data.get("vi_translation")
        or ""
    )

    sentiment_score = data.get("sentiment_score", 0.0) or 0.0
    if sentiment_score > 0.1:
        sentiment_label = "Positive"
    elif sentiment_score < -0.1:
        sentiment_label = "Negative"
    else:
        sentiment_label = "Neutral"

    def _extract_keywords_fallback(text: str, limit: int = 5):
        words = [
            w.strip(".,;:!?()[]{}\"'").lower()
            for w in (text or "").split()
            if len(w.strip(".,;:!?()[]{}\"'")) > 3
        ]
        uniq = []
        for w in words:
            if w and w not in uniq:
                uniq.append(w)
            if len(uniq) >= limit:
                break
        return uniq

    keywords = data.get("keywords") or _extract_keywords_fallback(summary_text)

    news_item = {
        "id": str(int(datetime.now().timestamp())),
        "source": data.get("source", "Live"),
        "title": data.get("headline_ocr") or summary_text[:120] or "Breaking News",
        "ocr_text": data.get("headline_ocr", "") or summary_text,
        "english_summary": summary_text,
        "vietnamese_translation": translation_text,
        "timestamp": data.get("timestamp"),
        "sentiment": sentiment_label,
        "keywords": keywords,
    }

    analytics = {
        "sentiment_score": sentiment_score,
        "trending_keywords": keywords,
        "active_sources": 1,
        "total_mentions": 1240,
    }

    subtitle = {
        "text": data.get("subtitle_vi", "Dang phan tich..."),
        "lang": "vi",
        "timestamp": data.get("timestamp"),
    }

    await manager.broadcast({"type": "news", "data": news_item})
    await manager.broadcast({"type": "analytics", "data": analytics})
    await manager.broadcast({"type": "subtitle", "data": subtitle})

    print(f"[WebSocket] Broadcasted: {subtitle['text'][:50]}...")

    # Persist to DB so KG worker can pick it up
    async def _write_to_db():
        from database import SessionLocal

        def _persist():
            db = SessionLocal()
            try:
                ts_str = data.get("timestamp")
                ts_val = None
                if ts_str:
                    try:
                        ts_val = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    except Exception:
                        ts_val = datetime.utcnow()
                if not summary_text or summary_text.strip() == "{":
                    return
                log = AnalysisLog(
                    source=news_item.get("source"),
                    summary=summary_text,
                    vietnamese_translation=translation_text,
                    raw_text=news_item.get("ocr_text", ""),
                    sentiment_score=sentiment_score,
                    trending_keywords=keywords,
                    video_timestamp=news_item.get("timestamp"),
                    timestamp=ts_val,
                )
                db.add(log)
                db.commit()
            except Exception as exc:
                print(f"[Ingestion->DB] Failed to persist log: {exc}")
                db.rollback()
            finally:
                db.close()

        await asyncio.to_thread(_persist)

    await _write_to_db()


async def _consume_ingestion_queue():
    if not ingestion_queue:
        print("Ingestion queue not configured; skipping consumer.")
        return
    print("Starting ingestion queue consumer...")
    while not _stop_event.is_set():
        try:
            item = await asyncio.to_thread(ingestion_queue.pop, 5)
            if not item:
                continue
            await process_callback(item)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[Queue consumer] Error: {exc}")
            await asyncio.sleep(1)
    print("Ingestion queue consumer stopped.")


@app.on_event("startup")
async def startup_event():
    global consumer_task, kg_worker_task
    print("Starting Media Monitor API Gateway...")
    if processing_enabled and ingestion_queue:
        consumer_task = asyncio.create_task(_consume_ingestion_queue())
    print("Starting KG worker...")
    kg_worker_task = asyncio.create_task(kg_worker.run())


@app.on_event("shutdown")
async def shutdown_event():
    global consumer_task, kg_worker_task
    print("Shutting down ingestion consumer...")
    _stop_event.set()
    if consumer_task:
        consumer_task.cancel()
    print("Stopping KG worker...")
    await kg_worker.stop()
    if kg_worker_task:
        kg_worker_task.cancel()


@app.get("/")
async def root():
    return {"message": "Media Monitor API Gateway is running"}


@app.post("/chat")
async def chat(request: ChatRequest, db: Session = Depends(get_db)):
    result = await answer_question(request.question, db)
    return result


@app.post("/crawl")
async def crawl_news(request: CrawlRequest, background_tasks: BackgroundTasks):
    keyword = (request.keyword or "").strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="Keyword is required")

    source_type = (request.source_type or "news").lower()
    do_news = source_type in ("news", "both")
    do_social = source_type in ("social", "both")
    req_lang = (request.lang or request.language or "en") if hasattr(request, "language") else (request.lang or "en")
    req_source_id = request.source_id or request.source
    req_sources = request.sources or ([] if not req_source_id else [req_source_id])

    news_items: list[dict] = []
    social_items: list[dict] = []

    async with httpx.AsyncClient(timeout=15.0) as client:
        if do_news:
            news_data_api_key = os.getenv("NEWS_DATA_API_KEY") or os.getenv("NEWSDATA_API_KEY") or os.getenv("NEWSDATA_IO_API_KEY")
            if news_data_api_key:
                params = {
                    "apikey": news_data_api_key,
                    "q": keyword,
                    "language": req_lang or "en",
                    "size": min(max(request.limit, 1), 20),
                }
                if request.country:
                    params["country"] = request.country.lower()
                if req_source_id:
                    params["source_id"] = req_source_id
                if req_sources:
                    params["source_id"] = ",".join(req_sources)
                url = "https://newsdata.io/api/1/latest"
                resp = await client.get(url, params=params)
                if resp.status_code != 200:
                    raise HTTPException(status_code=resp.status_code, detail=resp.text)
                payload = resp.json()
                articles = payload.get("results") or []
                news_items = [
                    {
                        "title": a.get("title"),
                        "description": a.get("description"),
                        "url": a.get("link"),
                        "source": a.get("source_id"),
                        "published_at": a.get("pubDate"),
                        "type": "news",
                    }
                    for a in articles
                ]
            else:
                api_key = os.getenv("NEWSAPI_KEY") or os.getenv("NEWSAPI_API_KEY")
                if not api_key:
                    raise HTTPException(status_code=500, detail="NEWSAPI_KEY or NEWS_DATA_API_KEY not configured")
                params = {
                    "q": keyword,
                    "language": req_lang or "en",
                    "sortBy": "publishedAt",
                    "pageSize": min(max(request.limit, 1), 20),
                    "apiKey": api_key,
                }
                if request.country:
                    params["country"] = request.country.lower()
                if req_sources:
                    params["sources"] = ",".join(req_sources)
                elif req_source_id:
                    params["sources"] = req_source_id
                url = "https://newsapi.org/v2/everything"
                resp = await client.get(url, params=params)
                if resp.status_code != 200:
                    raise HTTPException(status_code=resp.status_code, detail=resp.text)
                payload = resp.json()
                articles = payload.get("articles", [])
                news_items = [
                    {
                        "title": a.get("title"),
                        "description": a.get("description"),
                        "url": a.get("url"),
                        "source": (a.get("source") or {}).get("name"),
                        "published_at": a.get("publishedAt"),
                        "type": "news",
                    }
                    for a in articles
                ]

        # Fixed RSS crawl
        rss_items: list[dict] = []
        rss_urls = DEFAULT_RSS_FEEDS
        for rss_url in rss_urls:
            try:
                feed = feedparser.parse(rss_url)
                # For demo: always take the first 1-2 posts from each feed, ignore keyword filtering
                for entry in feed.entries[:2]:
                    title = entry.get("title") or ""
                    desc = entry.get("summary") or ""
                    rss_items.append(
                        {
                            "title": title,
                            "description": desc,
                            "url": entry.get("link"),
                            "source": feed.feed.get("title") if hasattr(feed, "feed") else "RSS",
                            "published_at": entry.get("published"),
                            "type": "rss",
                        }
                    )
            except Exception as exc:
                print(f"[RSS] Failed to parse {rss_url}: {exc}")

        if do_social:
            token = os.getenv("X_BEARER_TOKEN")
            if not token:
                raise HTTPException(status_code=500, detail="X_BEARER_TOKEN not configured")
            params = {
                "query": keyword,
                "max_results": min(max(request.limit * 3, 10), 100),
                "tweet.fields": "created_at,lang,public_metrics,text",
            }
            url = "https://api.twitter.com/2/tweets/search/recent"
            resp = await client.get(url, params=params, headers={"Authorization": f"Bearer {token}"})
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            payload = resp.json()
            tweets = payload.get("data", []) or []
            social_items = [
                {
                    "title": (t.get("text") or "")[:120],
                    "description": t.get("text"),
                    "url": f"https://x.com/i/web/status/{t.get('id')}",
                    "source": "X",
                    "published_at": t.get("created_at"),
                    "type": "social",
                }
                for t in tweets
            ]

    # Combine rss with news for return + optional Neo4j upsert
    if rss_items:
        if news_items:
            news_items.extend(rss_items)
        else:
            news_items = rss_items

    # Translate crawled content to Vietnamese (best effort, skipped if no translation API key is configured)
    translation_api_key = os.getenv("GROQ_API_KEY") or os.getenv("OPENAI_API_KEY")
    translation_client = (
        OpenAI(
            api_key=translation_api_key,
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.groq.com/openai/v1"),
        )
        if translation_api_key
        else None
    )
    if translation_client:
        for item in news_items:
            combined_text = " ".join(
                [
                    str(item.get("title") or "").strip(),
                    str(item.get("description") or "").strip(),
                ]
            ).strip()
            text_for_translation = combined_text or item.get("description") or item.get("title") or ""
            translated = _translate_to_vi(text_for_translation, client=translation_client)
            if translated:
                item["vietnamese_translation"] = translated
        for item in social_items:
            text_for_translation = item.get("description") or item.get("title") or ""
            translated = _translate_to_vi(text_for_translation, client=translation_client)
            if translated:
                item["vietnamese_translation"] = translated

    if neo4j_client and news_items:
        terms = [t.strip() for t in request.keyword.split(",")] if request.keyword else []
        background_tasks.add_task(_background_upsert_crawled_news, news_items, terms)

    # Persist crawled items to AnalysisLog (ignore malformed summaries)
    def _persist_crawl_items(items: list[dict]):
        if not items:
            return
        db = SessionLocal()
        try:
            for item in items:
                summary_text = item.get("title") or item.get("description") or ""
                if not summary_text or summary_text.strip() == "{":
                    continue
                ts_val = None
                ts_str = item.get("published_at")
                if ts_str:
                    try:
                        ts_val = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    except Exception:
                        ts_val = None
                log = AnalysisLog(
                    source=item.get("source"),
                    summary=summary_text,
                    vietnamese_translation=item.get("vietnamese_translation"),
                    raw_text=item.get("description") or "",
                    sentiment_score=0.0,
                    trending_keywords=[],
                    video_timestamp=None,
                    timestamp=ts_val,
                )
                db.add(log)
            db.commit()
        except Exception as exc:
            print(f"[Crawl->DB] Failed to persist crawled items: {exc}")
            db.rollback()
        finally:
            db.close()

    # Save news + rss + social items (they're already combined for news_items)
    try:
        _persist_crawl_items(news_items + social_items)
    except Exception as exc:
        print(f"[Crawl] Persist error: {exc}")

    return {
        "source_type": source_type,
        "news": {"count": len(news_items), "items": news_items},
        "social": {"count": len(social_items), "items": social_items},
        "rss": {"count": len(rss_items), "items": rss_items},
    }


@app.post("/whisper/transcribe")
async def whisper_transcribe(file: UploadFile = File(...)):
    client = _get_audio_client()
    if not client:
        raise HTTPException(
            status_code=500,
            detail="Missing GROQ_API_KEY/OPENAI_API_KEY for Whisper transcription",
        )

    audio_bytes = await file.read()
    if not audio_bytes:
        raise HTTPException(status_code=400, detail="Empty audio file")

    audio_file = BytesIO(audio_bytes)
    audio_file.name = file.filename or "audio.webm"
    model_name = os.getenv("WHISPER_MODEL", "whisper-large-v3")

    try:
        transcription = client.audio.transcriptions.create(
            model=model_name,
            file=audio_file,
            response_format="verbose_json",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Whisper transcription failed: {exc}") from exc

    payload = (
        transcription.model_dump()
        if hasattr(transcription, "model_dump")
        else transcription
        if isinstance(transcription, dict)
        else {}
    )

    text = payload.get("text") or getattr(transcription, "text", "")
    language = payload.get("language") or getattr(transcription, "language", None)
    duration = payload.get("duration") or getattr(transcription, "duration", None)

    segments = []
    for seg in payload.get("segments") or []:
        if hasattr(seg, "model_dump"):
            seg = seg.model_dump()
        if isinstance(seg, dict):
            segments.append(
                {
                    "id": seg.get("id"),
                    "start": seg.get("start"),
                    "end": seg.get("end"),
                    "text": seg.get("text"),
                    "avg_logprob": seg.get("avg_logprob"),
                }
            )

    now_ts = datetime.now().isoformat()
    news_item = {
        "id": f"whisper-{int(datetime.now().timestamp())}",
        "source": "Mic Recording",
        "title": text[:120] or "Recorded audio",
        "ocr_text": text,
        "english_summary": text,
        "vietnamese_translation": "",
        "timestamp": now_ts,
        "sentiment": "Neutral",
        "keywords": [],
        "summary": text,
    }

    return {
        "text": text,
        "language": language,
        "duration": duration,
        "segments": segments,
        "news_item": news_item if text else None,
    }


# --- Person service proxy helpers ---


async def _proxy_people_search(q: str, limit: int = 5, include_social: bool = False):
    params = {"q": q, "limit": limit, "include_social": include_social}
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(f"{PERSON_SERVICE_URL}/people/search", params=params)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


async def _proxy_people_search_face(file: UploadFile, limit: int = 5, include_social: bool = False):
    form = {"file": (file.filename or "face.jpg", await file.read(), file.content_type or "image/jpeg")}
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(
            f"{PERSON_SERVICE_URL}/people/search/face",
            params={"limit": limit, "include_social": include_social},
            files=form,
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


async def _proxy_person_social_crawl(person_id: str, confirm: bool = False):
    params = {"confirm": str(bool(confirm)).lower()}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{PERSON_SERVICE_URL}/people/{person_id}/social-crawl", params=params)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


@app.get("/people/search")
async def proxy_people_search(q: str, limit: int = 5, include_social: bool = False):
    """Proxy to person-service search (text)."""
    return await _proxy_people_search(q, limit, include_social)


@app.post("/people/search/face")
async def proxy_people_search_face(file: UploadFile = File(...), limit: int = 5, include_social: bool = False):
    """Proxy to person-service face search."""
    return await _proxy_people_search_face(file, limit, include_social)


@app.get("/people/social/preview")
async def person_social_preview(person_id: str = Query(..., description="Person ID")):
    """Get suggested X account/social graph for a person (no Neo4j writes)."""
    if not person_id:
        raise HTTPException(status_code=400, detail="person_id is required")
    return await _proxy_person_social_crawl(person_id, confirm=False)


@app.post("/people/social/crawl")
async def person_social_crawl(person_id: str = Query(..., description="Person ID")):
    """Confirm and persist social graph into Neo4j, returning KG cypher."""
    if not person_id:
        raise HTTPException(status_code=400, detail="person_id is required")
    data = await _proxy_person_social_crawl(person_id, confirm=True)
    person = data.get("person") or {"id": person_id}
    social_graph = data.get("social_graph") or {}
    if social_graph:
        try:
            _write_social_graph_to_neo4j(social_graph)
        except Exception as exc:
            # Surface Neo4j failures so UI knows crawl did not persist
            raise HTTPException(status_code=503, detail=f"Failed to write social graph: {exc}") from exc
    cypher = None
    if person.get("name"):
        escaped_name = str(person["name"]).replace('"', '\\"')
        cypher = f'''
        MATCH (p:Person)
        WHERE toLower(p.name) = toLower("{escaped_name}")
        OPTIONAL MATCH (p)-[r1:POSTED]->(po:Post)
        OPTIONAL MATCH (po)<-[r2:COMMENTED_ON]-(a:Account)
        RETURN p, po, r1, r2, a
        LIMIT 200
        '''
    return {
        "person": person,
        "social_graph": social_graph,
        "cypher": cypher,
        "status": data.get("status"),
        "primary_account": data.get("primary_account"),
        "message": data.get("message"),
    }


@app.get("/people/crawl-kg")
async def crawl_person_kg(person_id: str = Query(..., description="Person ID"), confirm: bool = False):
    """
    Two-step social crawl:
    - confirm=false: return suggested X account/social graph (no DB write)
    - confirm=true: write social graph to Neo4j and return KG cypher
    """
    if not person_id:
        raise HTTPException(status_code=400, detail="person_id is required")
    data = await _proxy_person_social_crawl(person_id, confirm=confirm)
    person = data.get("person") or {"id": person_id}
    social_graph = data.get("social_graph") or {}
    if confirm and social_graph:
        _write_social_graph_to_neo4j(social_graph)
    cypher = None
    if person.get("name"):
        escaped_name = str(person["name"]).replace('"', '\\"')
        cypher = f'''
        MATCH (n:Person)
        WHERE toLower(n.name) = toLower("{escaped_name}")
        OPTIONAL MATCH (n)-[r]-(m)
        RETURN n, r, m
        LIMIT 200
        '''
    return {
        "person": person,
        "social_graph": social_graph,
        "cypher": cypher,
        "status": data.get("status"),
        "primary_account": data.get("primary_account"),
        "message": data.get("message"),
    }


@app.get("/news/sources")
async def news_sources(country: str = "kh"):
    """List news sources (default Cambodia) from NewsData.io."""
    api_key = os.getenv("NEWS_DATA_API_KEY") or os.getenv("NEWSDATA_API_KEY") or os.getenv("NEWSDATA_IO_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="NEWS_DATA_API_KEY is required for sources lookup")
    url = "https://newsdata.io/api/1/sources"
    params = {"apikey": api_key, "country": country.lower()}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        payload = resp.json()
    return payload.get("results") or []


@app.get("/kg/search")
async def kg_search(keyword: str = Query(""), limit: int = Query(50, le=100)):
    if not neo4j_client:
        raise HTTPException(status_code=503, detail="Neo4j not configured")
    cypher = """
    MATCH p=(n:Entity)-[r]-(m)
    WHERE toLower(n.name) CONTAINS toLower($keyword)
    RETURN DISTINCT elementId(n) as nid, labels(n) as nlabels, n.name as nname, n.type as ntype,
                    elementId(m) as mid, labels(m) as mlabels, m.name as mname, m.type as mtype,
                    elementId(r) as rid, type(r) as rtype, elementId(startNode(r)) as sid, elementId(endNode(r)) as eid
    LIMIT $limit
    """
    with neo4j_client.session() as session:
        records = session.run(cypher, keyword=keyword, limit=limit).data()

    nodes = {}
    edges = {}
    for rec in records:
        nodes[rec["nid"]] = {
            "id": rec["nid"],
            "name": rec.get("nname") or rec["nid"],
            "type": rec.get("ntype") or "Entity",
            "labels": rec.get("nlabels") or [],
        }
        nodes[rec["mid"]] = {
            "id": rec["mid"],
            "name": rec.get("mname") or rec["mid"],
            "type": rec.get("mtype") or "Entity",
            "labels": rec.get("mlabels") or [],
        }
        rid = rec["rid"]
        if rid not in edges:
            edges[rid] = {
                "id": rid,
                "source": rec["sid"],
                "target": rec["eid"],
                "type": rec.get("rtype") or "",
            }

    return {"nodes": list(nodes.values()), "edges": list(edges.values())}


@app.get("/kg/person-analysis")
async def kg_person_analysis(node_id: str = Query(..., description="Person elementId/person_id/name identifier")):
    """Generate a Vietnamese analysis report for a person node and its close social graph."""
    person, level1_nodes, level2_nodes = _get_person_graph(node_id)
    person_profile = _fetch_person_profile(person)
    mock_payload = _load_mock_person_report(person)
    if mock_payload:
        # Add artificial latency so mocked responses resemble live processing time.
        await asyncio.sleep(10)
        return {
            "person": mock_payload.get("person") or person,
            "level1_nodes": mock_payload.get("level1_nodes") or level1_nodes,
            "level2_nodes": mock_payload.get("level2_nodes") or level2_nodes,
            "person_profile": mock_payload.get("person_profile") or person_profile,
            "report": mock_payload.get("report") or _generate_fallback_report(person, level1_nodes, level2_nodes),
        }
    report = _generate_graph_report(person, level1_nodes, level2_nodes, person_profile=person_profile)
    return {
        "person": person,
        "level1_nodes": level1_nodes,
        "level2_nodes": level2_nodes,
        "person_profile": person_profile,
        "report": report,
    }


@app.get("/documents")
async def list_documents():
    db = SessionLocal()
    try:
        docs = db.query(DocumentRecord).order_by(DocumentRecord.created_at.desc()).all()
        return [_document_to_dict(doc) for doc in docs]
    finally:
        db.close()


@app.get("/documents/{doc_id}")
async def get_document(doc_id: int):
    db = SessionLocal()
    try:
        rec = db.query(DocumentRecord).filter(DocumentRecord.id == doc_id).first()
        if not rec:
            raise HTTPException(status_code=404, detail="Document not found")
        return _document_to_dict(rec)
    finally:
        db.close()


@app.get("/documents/{doc_id}/images")
async def document_images(
    doc_id: int,
    limit: int = Query(6, ge=1, le=30, description="Maximum number of images to extract"),
):
    db = SessionLocal()
    try:
        rec = db.query(DocumentRecord).filter(DocumentRecord.id == doc_id).first()
        if not rec:
            raise HTTPException(status_code=404, detail="Document not found")
    finally:
        db.close()
    cached_preview = []
    if rec.metadata_json:
        cached_preview = rec.metadata_json.get("preview_images") or []
    if cached_preview:
        subset = cached_preview[:limit]
        return {"document_id": doc_id, "count": len(subset), "images": subset}
    pdf_path = Path(rec.storage_path)
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Source file not found")
    try:
        images = _extract_pdf_images(pdf_path, max_images=limit)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to extract images: {exc}") from exc
    return {"document_id": doc_id, "count": len(images), "images": images}


@app.get("/documents/{doc_id}/file")
async def download_document_file(doc_id: int):
    db = SessionLocal()
    try:
        rec = db.query(DocumentRecord).filter(DocumentRecord.id == doc_id).first()
        if not rec:
            raise HTTPException(status_code=404, detail="Document not found")
    finally:
        db.close()
    path = Path(rec.storage_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        path,
        media_type="application/pdf",
        filename=rec.original_name or path.name,
        content_disposition_type="inline",
    )


@app.post("/documents/upload")
async def upload_document(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF uploads are supported")
    blob = await file.read()
    if not blob:
        raise HTTPException(status_code=400, detail="Empty file")
    doc_name = f"{uuid.uuid4().hex}.pdf"
    storage_path = DOCUMENT_STORE / doc_name
    try:
        storage_path.write_bytes(blob)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to store document: {exc}") from exc

    parsed = _extract_pdf_metadata(storage_path)
    summary = await _summarize_document_text(parsed.get("full_text", ""))
    db = SessionLocal()
    try:
        rec = DocumentRecord(
            filename=doc_name,
            storage_path=str(storage_path),
            original_name=file.filename or doc_name,
            pages=parsed.get("pages") or 0,
            summary=summary,
            text_excerpt=parsed.get("excerpt"),
            metadata_json={
                "pdf_metadata": parsed.get("metadata") or {},
                "marker_metadata": parsed.get("marker_metadata") or {},
                "size_bytes": len(blob),
                "ocr_text": parsed.get("ocr_text") or "",
                "preview_images": parsed.get("preview_images") or [],
                "text_source": parsed.get("text_source"),
            },
        )
        db.add(rec)
        db.commit()
        db.refresh(rec)
        return _document_to_dict(rec)
    except Exception as exc:
        db.rollback()
        try:
            storage_path.unlink()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Failed to persist document: {exc}") from exc
    finally:
        db.close()


@app.delete("/documents/{doc_id}")
async def delete_document(doc_id: int):
    db = SessionLocal()
    try:
        rec = db.query(DocumentRecord).filter(DocumentRecord.id == doc_id).first()
        if not rec:
            raise HTTPException(status_code=404, detail="Document not found")
        db.delete(rec)
        db.commit()
        try:
            Path(rec.storage_path).unlink()
        except Exception:
            pass
        return {"status": "deleted", "id": doc_id}
    finally:
        db.close()


@app.post("/documents/{doc_id}/extract-kg")
async def extract_document_kg(doc_id: int):
    db = SessionLocal()
    try:
        rec = db.query(DocumentRecord).filter(DocumentRecord.id == doc_id).first()
        if not rec:
            raise HTTPException(status_code=404, detail="Document not found")
    finally:
        db.close()
    api_key = os.getenv("GROQ_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="Missing GROQ_API_KEY/OPENAI_API_KEY")
    client = OpenAI(
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.groq.com/openai/v1"),
    )
    summary_text = rec.summary or rec.text_excerpt or ""
    if not summary_text:
        path = Path(rec.storage_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="Source file missing for extraction")
        try:
            parsed = _extract_pdf_metadata(path)
            summary_text = parsed.get("excerpt") or parsed.get("full_text") or ""
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to read document: {exc}") from exc
    kg_data = await _extract_entities_from_summary(summary_text, rec.metadata_json or {}, client)
    neo4j_status = False
    if neo4j_client:
        doc_hash = int(hashlib.md5(f"document-{rec.id}".encode("utf-8")).hexdigest()[:16], 16) % (2**63 - 1)
        try:
            neo4j_client.upsert_kg_item(
                news_id=doc_hash,
                kg_data=kg_data,
                source="OCR Document",
                summary=rec.summary or rec.original_name,
                translation_vi=rec.summary or "",
                timestamp=datetime.utcnow().isoformat(),
                keywords=[],
            )
            neo4j_status = True
        except Exception as exc:
            print(f"[Documents] Neo4j upsert failed: {exc}")
    return {"kg": kg_data, "neo4j_upserted": neo4j_status}


@app.get("/asr/status")
async def asr_status():
    if not ingestion_queue:
        raise HTTPException(status_code=503, detail="Ingestion queue not configured")
    cfg = ingestion_queue.load_config()
    defaults = {
        "sources": [s.strip() for s in (os.getenv("WHISPER_SOURCES") or "").split(",") if s.strip()],
        "capture_seconds": int(os.getenv("WHISPER_CAPTURE_SECONDS", "45")),
        "break_seconds": int(os.getenv("WHISPER_BREAK_SECONDS", "15")),
        "capture_rate": int(os.getenv("WHISPER_CAPTURE_RATE", "1")),
        "enabled": True,
    }
    merged = {**defaults, **{k: v for k, v in cfg.items() if v is not None}}
    merged["running"] = processing_enabled
    return merged


@app.post("/asr/config")
async def asr_config(cfg: dict):
    if not ingestion_queue:
        raise HTTPException(status_code=503, detail="Ingestion queue not configured")
    allowed_keys = {"sources", "capture_seconds", "break_seconds", "capture_rate", "enabled"}
    clean = {k: v for k, v in cfg.items() if k in allowed_keys}
    ingestion_queue.save_config(clean)
    updated = ingestion_queue.load_config()
    updated["running"] = processing_enabled
    return updated


@app.websocket("/ws/monitor")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.post("/live/toggle")
async def live_toggle(req: LiveToggleRequest):
    global processing_enabled, consumer_task
    if req.enabled == processing_enabled:
        return {"enabled": processing_enabled}

    if not req.enabled:
        processing_enabled = False
        _stop_event.set()
        if consumer_task:
            consumer_task.cancel()
            consumer_task = None
        if ingestion_queue:
            cfg = ingestion_queue.load_config() or {}
            cfg["enabled"] = False
            ingestion_queue.save_config(cfg)
        return {"enabled": False}

    processing_enabled = True
    _stop_event.clear()
    if ingestion_queue:
        cfg = ingestion_queue.load_config() or {}
        cfg["enabled"] = True
        ingestion_queue.save_config(cfg)
        consumer_task = asyncio.create_task(_consume_ingestion_queue())
        # If queue is empty, send last 5 news items immediately to clients
        try:
            if ingestion_queue.length() == 0:
                db = SessionLocal()
                try:
                    recent = (
                        db.query(AnalysisLog)
                        .order_by(AnalysisLog.timestamp.desc())
                        .limit(5)
                        .all()
                    )
                    for log in reversed(recent):
                        msg = {
                            "type": "news",
                            "data": {
                                "id": str(log.id),
                                "source": log.source or "Live",
                                "title": log.summary or "",
                                "ocr_text": log.raw_text or "",
                                "english_summary": log.summary or "",
                                "vietnamese_translation": log.vietnamese_translation or "",
                                "timestamp": log.timestamp.isoformat() if log.timestamp else None,
                                "sentiment": "Neutral",
                                "keywords": log.trending_keywords or [],
                            },
                        }
                        await manager.broadcast(msg)
                finally:
                    db.close()
        except Exception as exc:
            print(f"[LiveToggle] Failed to send recent news: {exc}")
    return {"enabled": True}


@app.get("/live/state")
async def live_state():
    """Return current live-processing state and ingestion config."""
    cfg = ingestion_queue.load_config() if ingestion_queue else {}
    return {
        "enabled": bool(processing_enabled),
        "ingestion_enabled": bool(cfg.get("enabled")) if cfg else bool(processing_enabled),
        "config": cfg or {},
    }
