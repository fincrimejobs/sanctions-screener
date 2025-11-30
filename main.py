# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List
import requests
import os

app = FastAPI()

# CORS - in prod, lock this to your specific Webflow domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ScreenerInput(BaseModel):
    name: str

def recursive_find(obj: Any, keys: List[str]):
    """Recursively search a nested dict/list for the first matching key (case-insensitive).
       Returns the value or None."""
    if obj is None:
        return None

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k and isinstance(k, str) and k.lower() in [kk.lower() for kk in keys]:
                return v
            # recurse
            val = recursive_find(v, keys)
            if val is not None:
                return val
    elif isinstance(obj, list):
        for item in obj:
            val = recursive_find(item, keys)
            if val is not None:
                return val
    return None

def coerce_score(s):
    try:
        return float(s)
    except Exception:
        return 0.0

@app.get("/")
def home():
    return {"message": "Screener API is running!"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    api_key = os.getenv("OPENSANCTIONS_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Server misconfigured: OPENSANCTIONS_KEY not set")

    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        }
    }

    try:
        resp = requests.post(url, json=payload, timeout=15)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {str(e)}")

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Upstream returned {resp.status_code}: {resp.text[:1000]}")

    try:
        data = resp.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Upstream returned non-JSON response")

    results = data.get("responses", {}).get("q1", {}).get("results", [])
    if not isinstance(results, list):
        results = []

    matches = []
    for r in results:
        # Basic fields
        name = r.get("caption") or r.get("name") or recursive_find(r, ["name", "caption"]) or item.name
        score = coerce_score(r.get("score", 0))

        datasets = r.get("datasets") or r.get("dataset") or recursive_find(r, ["datasets", "dataset", "lists"]) or []
        # Ensure list
        if isinstance(datasets, str):
            datasets = [datasets]
        if not isinstance(datasets, list):
            datasets = list(datasets) if datasets else []

        # Sources: try multiple possible places
        sources = []
        if r.get("sources"):
            sources = r.get("sources")
        else:
            # try entity or record nested data
            e = r.get("entity") or r.get("record") or r
            s = recursive_find(e, ["sources", "source", "urls", "url", "links", "link"])
            if s:
                if isinstance(s, list):
                    sources = s
                elif isinstance(s, str):
                    sources = [s]

        # Try to extract identity details (DOB, nationality, aliases)
        dob = recursive_find(r, ["birth_date", "date_of_birth", "dob", "birthdate"])
        nationality = recursive_find(r, ["nationality", "country", "citizenship", "country_of_residence", "citizenships"])
        aliases = recursive_find(r, ["other_names", "aliases", "aka", "alternate_names", "names"])
        # normalize aliases to list of strings
        if aliases:
            if isinstance(aliases, str):
                aliases = [aliases]
            elif isinstance(aliases, dict):
                # try to pull the name fields
                aliases = [v for v in aliases.values() if isinstance(v, str)]
            elif isinstance(aliases, list):
                # ok
                aliases = [a for a in aliases if isinstance(a, str)]
            else:
                aliases = []

        # place_of_birth
        pob = recursive_find(r, ["birth_place", "place_of_birth", "born_in"])

        # Build match entry
        matches.append({
            "name": name,
            "score": score,
            "datasets": datasets,
            "sources": sources,
            "identity": {
                "date_of_birth": dob,
                "place_of_birth": pob,
                "nationality": nationality,
                "aliases": aliases or []
            },
            # include raw result for advanced UI/dev debugging (optional, small)
            # Remove or limit in production if payloads are large
            "raw": {k: v for k, v in r.items() if k in ("caption", "score", "datasets")}
        })

    status = "hit" if len([m for m in matches if m.get("score", 0) > 0.7]) > 0 else "clean"

    return {
        "status": status,
        "query": item.name,
        "matches": matches
    }
