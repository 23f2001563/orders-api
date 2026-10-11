import base64
import math
import time
import uuid
from collections import deque
from typing import Optional

from fastapi import FastAPI, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL = 51            # fixed catalog: order IDs 1..TOTAL
RATE_LIMIT = 15       # requests per client per window
WINDOW = 10.0         # seconds
DEFAULT_LIMIT = 10
MAX_LIMIT = 100

app = FastAPI()

CATALOG = [{"id": i, "item": f"item-{i}", "quantity": (i % 5) + 1} for i in range(1, TOTAL + 1)]

_hits: dict = {}              # client id -> deque of request timestamps (sliding window)
_orders_by_key: dict = {}     # idempotency key -> created order


def _now() -> float:
    return time.monotonic()


# ---------- per-client rate limiting ----------
# Added BEFORE the CORS middleware so CORS is the outermost layer and 429s carry CORS headers.
@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client = (request.headers.get("x-client-id") or "").strip()
    if request.method == "OPTIONS" or not client:
        return await call_next(request)

    now = _now()
    if len(_hits) > 10000:  # housekeeping: drop clients with no recent requests
        for cid in [c for c, d in _hits.items() if not d or now - d[-1] >= WINDOW]:
            del _hits[cid]

    window = _hits.setdefault(client, deque())
    while window and now - window[0] >= WINDOW:
        window.popleft()
    if len(window) >= RATE_LIMIT:
        retry_after = max(1, math.ceil(window[0] + WINDOW - now))
        return JSONResponse(
            status_code=429,
            content={"detail": "rate limit exceeded"},
            headers={"Retry-After": str(retry_after)},
        )
    window.append(now)
    return await call_next(request)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotent-Replayed"],
)


# ---------- idempotent POST ----------
@app.post("/orders", status_code=201)
async def create_order(request: Request, idempotency_key: Optional[str] = Header(None)):
    if not idempotency_key or not idempotency_key.strip():
        return JSONResponse(status_code=400, content={"detail": "Idempotency-Key header is required"})
    key = idempotency_key.strip()

    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict):
        body = {}

    # No await between lookup and store, so this check-and-set is atomic on the event loop.
    existing = _orders_by_key.get(key)
    if existing is not None:
        return JSONResponse(status_code=201, content=existing, headers={"Idempotent-Replayed": "true"})

    order = {k: v for k, v in body.items() if k not in ("id", "status")}
    order["id"] = str(uuid.uuid4())
    order["status"] = "created"
    _orders_by_key[key] = order
    return order


# ---------- cursor pagination ----------
def _encode(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode().rstrip("=")


def _decode(cursor: str) -> int:
    raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    if not raw.startswith("o:"):
        raise ValueError
    offset = int(raw[2:])
    if offset < 0:
        raise ValueError
    return offset


@app.get("/orders")
def list_orders(limit: int = Query(DEFAULT_LIMIT, ge=1), cursor: Optional[str] = None):
    limit = min(limit, MAX_LIMIT)
    offset = 0
    if cursor:
        try:
            offset = _decode(cursor)
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "invalid cursor"})
    page = CATALOG[offset:offset + limit]
    next_offset = offset + len(page)
    return {"items": page, "next_cursor": _encode(next_offset) if next_offset < TOTAL else None}


@app.get("/")
def root():
    return {"status": "ok"}