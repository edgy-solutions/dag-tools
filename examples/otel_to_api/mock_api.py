"""Mock target API for the otel_to_api example.

Deliberately reproduces the two behaviours the pipeline is built around:

* ``PATCH /api/EntityMaintenance/{id}`` 404s for an entity it has never
  seen, which is what triggers the aggregate fallback;
* the bulk ``POST /api/EntityMaintenance`` has **replace** semantics for
  the entities it receives — so if the fallback ever sent the whole
  group, the damage would be visible in ``GET /_state``.
"""
from typing import Any, Dict, List

from fastapi import FastAPI, Header, HTTPException, Request, Response
from pydantic import BaseModel

app = FastAPI()

# Seen idempotency keys -> the response first returned for them.
#
# Restate makes the journal exactly-once, but a side effect inside
# ctx.run is at-least-once at the crash boundary: kill the worker after
# a request lands but before its result is journaled and replay re-sends
# it. A live kill test produced exactly one extra call out of 802. This
# is the server-side half of the fix — the handler stamps a stable
# per-call key, and honouring it is what makes the end-to-end effect
# exactly-once for append-style endpoints.
IDEMPOTENCY: Dict[str, Any] = {}


@app.middleware("http")
async def idempotency(request: Request, call_next):
    key = request.headers.get("idempotency-key")
    if key and key in IDEMPOTENCY:
        STATE["replays_suppressed"] = STATE.get("replays_suppressed", 0) + 1
        return Response(
            content='{"replayed": true}',
            media_type="application/json",
            status_code=IDEMPOTENCY[key],
        )
    response = await call_next(request)
    if key:
        IDEMPOTENCY[key] = response.status_code
    return response

STATE: Dict[str, Any] = {
    "entities": {"entity_1": {"artifacts": "", "notes": "pre-existing, must survive"}},
    "items": {},
    "item_entity_map": {},
    "executions": [],
    "calls": [],
    "replays_suppressed": 0,
}


def _record(name: str, payload: Any) -> None:
    STATE["calls"].append({"call": name, "payload": payload})


class EntityPatch(BaseModel):
    artifacts: str = ""


@app.patch("/api/EntityMaintenance/{entity_id}")
def patch_entity(entity_id: str, body: EntityPatch):
    _record(f"PATCH /api/EntityMaintenance/{entity_id}", body.model_dump())
    if entity_id not in STATE["entities"]:
        raise HTTPException(status_code=404, detail="unknown entity")
    STATE["entities"][entity_id]["artifacts"] = body.artifacts
    return {"ok": True}


class EntityCreate(BaseModel):
    deleteMissingEntities: bool = False
    entities: List[Dict[str, Any]] = []


@app.post("/api/EntityMaintenance")
def create_entities(body: EntityCreate):
    _record("POST /api/EntityMaintenance", body.model_dump())
    if body.deleteMissingEntities:
        sent = {e["entityIdentifier"] for e in body.entities}
        for existing in list(STATE["entities"]):
            if existing not in sent:
                del STATE["entities"][existing]
    for entity in body.entities:
        # Replace semantics: any field not sent is lost. This is why the
        # fallback must only ever carry entities that actually 404'd.
        STATE["entities"][entity["entityIdentifier"]] = {
            "artifacts": entity.get("artifacts", "")
        }
    return {"created": len(body.entities)}


class ItemBulk(BaseModel):
    deleteMissingEntries: bool = False
    items: List[Dict[str, Any]] = []


@app.post("/api/ProcessItemDetails/BulkUpdate")
def bulk_items(body: ItemBulk):
    _record("POST /api/ProcessItemDetails/BulkUpdate", body.model_dump())
    for item in body.items:
        STATE["items"][item["itemName"]] = item
    return {"upserted": len(body.items)}


class EntityMapping(BaseModel):
    entityIdentifiers: List[str] = []


@app.post("/api/ProcessItemDetails/{item_name}/EntityMapping")
def map_item(item_name: str, body: EntityMapping):
    _record(f"POST /api/ProcessItemDetails/{item_name}/EntityMapping", body.model_dump())
    STATE["item_entity_map"][item_name] = body.entityIdentifiers
    return {"mapped": len(body.entityIdentifiers)}


@app.post("/api/RecordExecution")
def record_execution(body: Dict[str, Any]):
    _record("POST /api/RecordExecution", body)
    # Append-style: duplicates here are exactly what the readiness gate,
    # the dispatch ledger and the object's completed-hash state prevent.
    STATE["executions"].append(body)
    return {"recorded": True}


@app.get("/_state")
def state():
    return STATE


@app.post("/_reset")
def reset():
    STATE["entities"] = {"entity_1": {"artifacts": "", "notes": "pre-existing, must survive"}}
    STATE["items"] = {}
    STATE["item_entity_map"] = {}
    STATE["executions"] = []
    STATE["calls"] = []
    STATE["replays_suppressed"] = 0
    IDEMPOTENCY.clear()
    return {"ok": True}
