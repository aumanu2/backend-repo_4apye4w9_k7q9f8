import os
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import csv
import io
from bson import ObjectId

from database import db, create_document, get_documents
from schemas import Dataset, Record

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"message": "Interactive Tables API is running"}


@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    """
    Accept a CSV upload, infer schema, store dataset + rows in MongoDB.
    Returns dataset metadata and a preview of the first 50 rows.
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    content = await file.read()
    try:
        text = content.decode("utf-8", errors="ignore")
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to decode file as UTF-8")

    reader = csv.DictReader(io.StringIO(text))
    columns = reader.fieldnames or []
    if not columns:
        raise HTTPException(status_code=400, detail="CSV has no header row")

    # Collect rows (limit for preview, but we will insert all)
    rows: List[Dict[str, Any]] = []
    preview_rows: List[Dict[str, Any]] = []

    # Infer column types simple heuristic
    def infer_type(value: str) -> str:
        if value is None or value == "":
            return "string"
        v = value.strip()
        if v.lower() in {"true", "false"}:
            return "boolean"
        try:
            int(v)
            return "number"
        except:
            pass
        try:
            float(v)
            return "number"
        except:
            pass
        # very light date detection
        if any(sep in v for sep in ["-", "/"]) and any(ch.isdigit() for ch in v):
            return "date"
        return "string"

    # Determine types by sampling first 100 rows
    type_counts: Dict[str, Dict[str, int]] = {c: {} for c in columns}
    max_preview = 50
    sample_limit = 100
    for i, row in enumerate(reader):
        rows.append(row)
        if len(preview_rows) < max_preview:
            preview_rows.append(row)
        if i < sample_limit:
            for c in columns:
                t = infer_type(row.get(c))
                type_counts[c][t] = type_counts[c].get(t, 0) + 1

    column_types: Dict[str, str] = {}
    for c in columns:
        if type_counts[c]:
            column_types[c] = max(type_counts[c].items(), key=lambda kv: kv[1])[0]
        else:
            column_types[c] = "string"

    # Create dataset document
    dataset = Dataset(
        name=file.filename,
        columns=columns,
        column_types=column_types,
        row_count=len(rows),
    )
    dataset_id = create_document("dataset", dataset)

    # Insert rows
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")
    recs = [{"dataset_id": dataset_id, "data": r} for r in rows]
    # Add timestamps like create_document
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    for r in recs:
        r["created_at"] = now
        r["updated_at"] = now
    if recs:
        db["record"].insert_many(recs)

    return {
        "dataset_id": dataset_id,
        "name": dataset.name,
        "columns": columns,
        "column_types": column_types,
        "row_count": len(rows),
        "preview": preview_rows,
    }


class QueryRequest(BaseModel):
    dataset_id: str
    query: str
    limit: int = 100


@app.post("/api/query")
async def query_dataset(req: QueryRequest):
    """
    Very simple natural-language filter using heuristics (no external LLMs).
    Supports expressions like:
    - "price > 100"
    - "country = US"
    - "status is true"
    - "contains name John"
    Multiple conditions can be joined with "and".
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")

    # Build MongoDB filter from naive parsing
    text = req.query.strip()
    if not text:
        filter_dict: Dict[str, Any] = {"dataset_id": req.dataset_id}
    else:
        clauses = [c.strip() for c in text.split(" and ") if c.strip()]
        filter_dict = {"dataset_id": req.dataset_id}
        and_parts = []
        for c in clauses:
            # patterns
            if c.lower().startswith("contains "):
                # contains column value
                # e.g., contains name John
                parts = c.split()
                if len(parts) >= 3:
                    col = parts[1]
                    val = " ".join(parts[2:])
                    and_parts.append({f"data.{col}": {"$regex": val, "$options": "i"}})
                continue
            for op in [">=", "<=", ">", "<", "=", "!=", " is "]:
                if op in c:
                    col, val = c.split(op, 1)
                    col = col.strip()
                    val = val.strip()
                    # normalize op
                    if op.strip() == "is":
                        op_tok = "="
                    else:
                        op_tok = op
                    # cast numbers/bool
                    if val.lower() in ["true", "false"]:
                        v_cast: Any = True if val.lower() == "true" else False
                    else:
                        try:
                            if "." in val:
                                v_cast = float(val)
                            else:
                                v_cast = int(val)
                        except:
                            v_cast = val
                    mongo_op_map = {
                        "=": "$eq",
                        "!=": "$ne",
                        ">": "$gt",
                        "<": "$lt",
                        ">=": "$gte",
                        "<=": "$lte",
                    }
                    if op_tok in mongo_op_map:
                        and_parts.append({f"data.{col}": {mongo_op_map[op_tok]: v_cast}})
                    break
        if and_parts:
            filter_dict = {"$and": [{"dataset_id": req.dataset_id}] + and_parts}

    docs = list(db["record"].find(filter_dict).limit(min(max(req.limit, 1), 1000)))
    rows = [d.get("data", {}) for d in docs]
    return {"rows": rows, "count": len(rows)}


@app.get("/api/datasets")
async def list_datasets():
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")
    docs = list(db["dataset"].find({}, {"name": 1, "columns": 1, "row_count": 1}))
    for d in docs:
        d["_id"] = str(d["_id"]) if "_id" in d else None
    return {"datasets": docs}


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


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
