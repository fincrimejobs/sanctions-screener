# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List, Tuple
import requests
import os
import logging
import itertools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("screener")

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
    if obj is None:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k and isinstance(k, str) and k.lower() in [kk.lower() for kk in keys]:
                return v
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

def normalize_result_record(r: Dict[str,Any]) -> Dict[str,Any]:
    # Try to produce the same normalized shape as your frontend expects
    name = r.get("caption") or r.get("name") or recursive_find(r, ["name", "caption"]) or ""
    score = coerce_score(r.get("score", 0))
    datasets = r.get("datasets") or r.get("dataset") or recursive_find(r, ["datasets", "dataset", "lists"]) or []
    if isinstance(datasets, str):
        datasets = [datasets]
    if not isinstance(datasets, list):
        try:
            datasets = list(datasets) if datasets else []
        except Exception:
            datasets = []

    # Sources
    sources = []
    if r.get("sources"):
        sources = r.get("sources")
    else:
        e = r.get("entity") or r.get("record") or r
        s = recursive_find(e, ["sources", "source", "urls", "url", "links", "link"])
        if s:
            if isinstance(s, list):
                sources = s
            elif isinstance(s, str):
                sources = [s]

    dob = recursive_find(r, ["birth_date", "date_of_birth", "dob", "birthdate"])
    nationality = recursive_find(r, ["nationality", "country", "citizenship", "country_of_residence", "citizenships"])
    aliases = recursive_find(r, ["other_names", "aliases", "aka", "alternate_names", "names"])
    if aliases:
        if isinstance(aliases, str):
            aliases = [aliases]
        elif isinstance(aliases, dict):
            aliases = [v for v in aliases.values() if isinstance(v, str)]
        elif isinstance(aliases, list):
            aliases = [a for a in aliases if isinstance(a, str)]
        else:
            aliases = []
    else:
        aliases = []

    pob = recursive_find(r, ["birth_place", "place_of_birth", "born_in"])

    return {
        "name": name,
        "score": score,
        "datasets": datasets,
        "sources": sources,
        "identity": {
            "date_of_birth": dob,
            "place_of_birth": pob,
            "nationality": nationality,
            "aliases": aliases
        },
        "raw": {k: v for k, v in r.items() if k in ("caption", "score", "datasets", "id")}
    }

@app.get("/")
def home():
    return {"message": "Screener API is running!"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    api_key = os.getenv("OPENSANCTIONS_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Server misconfigured: OPENSANCTIONS_KEY not set")

    try:
        MAX_RESULTS = int(os.getenv("OPENSANCTIONS_MAX_RESULTS", "50"))
    except Exception:
        MAX_RESULTS = 50

    match_url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    search_url = f"https://api.opensanctions.org/search?api_key={api_key}"

    # 1) Try match endpoint first (more precise, but often limited)
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        },
        "options": {"maximumResults": MAX_RESULTS}
    }

    logger.info("Calling match endpoint, requested max %s for name: %s", MAX_RESULTS, item.name)
    try:
        resp = requests.post(match_url, json=payload, timeout=30)
    except requests.RequestException as e:
        logger.exception("Upstream match request failed")
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {str(e)}")

    if resp.status_code != 200:
        logger.error("Match endpoint returned non-200: %s", resp.status_code)
        # return a helpful error
        raise HTTPException(status_code=502, detail=f"Upstream returned {resp.status_code}: {resp.text[:1000]}")

    try:
        data = resp.json()
    except ValueError:
        logger.error("Match endpoint returned non-JSON")
        raise HTTPException(status_code=502, detail="Upstream returned non-JSON response")

    # extract match results robustly
    match_results = []
    try:
        match_results = data.get("responses", {}).get("q1", {}).get("results", [])
    except Exception:
        match_results = []

    if not isinstance(match_results, list):
        if isinstance(data, list):
            match_results = data
        else:
            match_results = data.get("matches") or data.get("results") or match_results

    match_results = match_results or []

    logger.info("Match endpoint returned %d result(s)", len(match_results))

    # If match_results already meets or exceeds MAX_RESULTS, use them and return
    normalized = [normalize_result_record(r) for r in match_results]
    unique_by_id = {}
    def record_key(r):
        # prefer raw.id when present, fallback to name+score
        raw_id = (r.get("raw") or {}).get("id")
        if raw_id:
            return f"id::{raw_id}"
        return f"name::{r.get('name','')}_score::{r.get('score',0)}"

    for r in normalized:
        unique_by_id[record_key(r)] = r

    total_collected = len(unique_by_id)

    used_search = False
    raw_results_count = len(match_results)

    # 2) If we didn't get enough results from match, call search endpoint as fallback
    if total_collected < MAX_RESULTS:
        logger.info("Match returned %d < requested %d — calling search endpoint as fallback", total_collected, MAX_RESULTS)
        used_search = True
        # attempt a search request (size param or pageSize)
        try:
            # Some OpenSanctions search endpoints accept 'size' or 'page[size]'; we try 'size' first.
            params = {"q": item.name, "size": MAX_RESULTS}
            sresp = requests.get(search_url, params=params, timeout=30)
            if sresp.status_code == 200:
                sdata = sresp.json()
                # search results may be in 'results' or top-level array
                sresults = sdata.get("results") if isinstance(sdata, dict) else []
                if not sresults and isinstance(sdata, list):
                    sresults = sdata
                # fallback: try other keys
                if not sresults and isinstance(sdata, dict):
                    for k in ("matches","hits","items"):
                        if isinstance(sdata.get(k), list):
                            sresults = sdata.get(k)
                            break
                # normalize and merge
                sresults = sresults or []
                raw_results_count += len(sresults)
                for r in sresults:
                    nr = normalize_result_record(r)
                    unique_by_id[record_key(nr)] = nr
                    if len(unique_by_id) >= MAX_RESULTS:
                        break
            else:
                logger.warning("Search endpoint returned non-200: %s", sresp.status_code)
        except requests.RequestException:
            logger.exception("Search fallback failed")

    # Prepare final matches list (limit to MAX_RESULTS)
    final_matches = list(unique_by_id.values())[:MAX_RESULTS]

    status_flag = "hit" if any(m.get("score",0) > 0.7 for m in final_matches) else "clean"

    return {
        "status": status_flag,
        "query": item.name,
        "matches": final_matches,
        # diagnostic info:
        "raw_results_count": raw_results_count,
        "requested_max_results": MAX_RESULTS,
        "used_search": used_search,
    }
