import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from database import db, create_document, get_documents
from schemas import Event

app = FastAPI(title="Lytikz API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Welcome to Lytikz — Event tracking and analytics"}

@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }
    try:
        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"

    response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"
    return response

# ------------------------- Lytikz Core -----------------------------

class IngestEventPayload(BaseModel):
    event: str
    user_id: Optional[str] = None
    properties: Dict[str, Any] = {}
    timestamp: Optional[datetime] = None

@app.post("/api/events")
def ingest_event(payload: IngestEventPayload):
    """Ingest an analytics event into the database"""
    try:
        evt = Event(**payload.model_dump())
        # Ensure timestamp is set
        if evt.timestamp is None:
            evt.timestamp = datetime.now(timezone.utc)
        inserted_id = create_document("event", evt)
        return {"status": "ok", "id": inserted_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/events")
def list_events(limit: int = Query(50, ge=1, le=500)):
    """List recent events"""
    try:
        docs = get_documents("event", {}, limit)
        # Convert ObjectId and datetime for JSON serialization
        def normalize(doc):
            d = dict(doc)
            if "_id" in d:
                d["id"] = str(d.pop("_id"))
            for k, v in list(d.items()):
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
            if isinstance(d.get("properties"), dict):
                # leave as is
                pass
            return d
        return {"items": [normalize(d) for d in docs]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class QueryPayload(BaseModel):
    filter: Optional[Dict[str, Any]] = None
    limit: int = 100

@app.post("/api/query")
def query_events(payload: QueryPayload):
    """Run a simple filter query over events"""
    try:
        filt = payload.filter or {}
        docs = get_documents("event", filt, payload.limit)
        def normalize(doc):
            d = dict(doc)
            if "_id" in d:
                d["id"] = str(d.pop("_id"))
            for k, v in list(d.items()):
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
            return d
        return {"items": [normalize(d) for d in docs]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Simple Q&A over data: schema-aware but not LLM-based; provide aggregates
class AskPayload(BaseModel):
    question: str
    event: Optional[str] = None

@app.post("/api/ask")
def ask_question(payload: AskPayload):
    """Answer simple analytic questions: counts per event, total events, by user.
    This is a rule-based MVP, not LLM. Examples:
    - "how many signup"
    - "events by event"
    - "count by user"
    """
    try:
        q = payload.question.lower().strip()
        collection = db["event"]
        # total count
        if "total" in q or "how many" in q and ("event" in q or payload.event):
            filt = {"event": payload.event} if payload.event else {}
            count = collection.count_documents(filt)
            return {"answer": f"Total events: {count}", "count": count}
        if "by user" in q or "per user" in q:
            pipeline = [
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 20}
            ]
            res = list(collection.aggregate(pipeline))
            items = [{"user_id": r.get("_id"), "count": r.get("count", 0)} for r in res]
            return {"answer": "Events by user", "items": items}
        if "by event" in q or "per event" in q or "events by name" in q:
            pipeline = [
                {"$group": {"_id": "$event", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 50}
            ]
            res = list(collection.aggregate(pipeline))
            items = [{"event": r.get("_id"), "count": r.get("count", 0)} for r in res]
            return {"answer": "Events by event name", "items": items}
        # default: recent events
        recent = list(collection.find({}, {"event": 1, "user_id": 1, "timestamp": 1}).sort("timestamp", -1).limit(20))
        for r in recent:
            r["id"] = str(r.pop("_id"))
            if isinstance(r.get("timestamp"), datetime):
                r["timestamp"] = r["timestamp"].isoformat()
        return {"answer": "Showing recent events", "items": recent}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
