# heatmap.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import os
import requests
import logging
import time

logger = logging.getLogger("heatmap")
router = APIRouter(prefix="/heatmap")

class HeatmapQuery(BaseModel):
    max_pages: Optional[int] = 4        # how many pages to fetch (safe default)
    page_size: Optional[int] = 100      # page size per request (safe default)
    q: Optional[str] = "*"              # search query (default wildcard)
    sample_per_country: Optional[int] = 5

def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default

def recursive_find(obj: Any, keys: List[str]):
    """Small helper to scan nested dict/list for the first match by key names."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            try:
                if isinstance(k, str) and k.lower() in [kk.lower() for kk in keys]:
                    return v
            except Exception:
                pass
            val = recursive_find(v, keys)
            if val is not None:
                return val
    elif isinstance(obj, list):
        for item in obj:
            val = recursive_find(item, keys)
            if val is not None:
                return val
    return None

def extract_nationality(record: Dict[str, Any]) -> Optional[str]:
    # look for common fields that contain a country or nationality
    cand = recursive_find(record, ["nationality", "country", "citizenship", "country_of_residence"])
    if isinstance(cand, list) and cand:
        return cand[0]
    if isinstance(cand, str) and cand.strip():
        return cand.strip()
    # addresses may contain country
    addr = recursive_find(record, ["addresses", "address"])
    if isinstance(addr, list):
        for a in addr:
            if isinstance(a, dict) and a.get("country"):
                return a.get("country")
            if isinstance(a, str) and len(a) > 2:
                return a
    return None

def extract_name(record: Dict[str, Any]) -> str:
    name = record.get("caption") or record.get("name") or recursive_find(record, ["name", "caption"]) or ""
    return str(name)

def extract_datasets(record: Dict[str, Any]) -> List[str]:
    ds = record.get("datasets") or record.get("dataset") or recursive_find(record, ["datasets", "dataset", "lists"])
    if not ds:
        return []
    if isinstance(ds, str):
        return [ds]
    if isinstance(ds, list):
        return [str(x) for x in ds if x]
    # fallback: try to convert iterable
    try:
        return list(ds)
    except Exception:
        return []

@router.get("/", summary="Heatmap summary (aggregates OpenSanctions results)")
def heatmap_get(max_pages: Optional[int] = 4, page_size: Optional[int] = 100, q: Optional[str] = "*"):
    """
    Simple GET wrapper so you can call /heatmap?max_pages=2&page_size=50&q=*
    Default q="*" (wildcard). Uses POST /search under the hood (OpenSanctions expects POST).
    """
    body = HeatmapQuery(max_pages=max_pages, page_size=page_size, q=q)
    return heatmap_post(body)

@router.post("/", summary="Heatmap - POST")
def heatmap_post(query: HeatmapQuery):
    api_key = os.getenv("OPENSANCTIONS_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Server misconfigured: OPENSANCTIONS_KEY not set")

    base_url = "https://api.opensanctions.org/search"
    used_url = base_url
    totals: Dict[str, int] = {}
    datasets_count: Dict[str, int] = {}
    samples: Dict[str, List[str]] = {}
    fetched = 0
    debug = {"first_response_snippet": None, "errors": []}
    max_pages = max(1, _safe_int(query.max_pages, 4))
    page_size = max(1, min(500, _safe_int(query.page_size, 100)))  # cap page_size to reasonable max
    q = query.q or "*"
    sample_per_country = max(1, _safe_int(query.sample_per_country, 5))

    headers = {"Content-Type": "application/json"}
    # You can include api_key as query param (same pattern used elsewhere in your project)
    params = {"api_key": api_key}

    for page in range(1, max_pages + 1):
        payload = {"q": q, "page": page, "size": page_size}
        try:
            resp = requests.post(base_url, params=params, json=payload, timeout=30)
        except requests.RequestException as e:
            logger.exception("OpenSanctions request failed")
            debug["errors"].append(str(e))
            break

        # capture first response snippet for debugging
        if debug["first_response_snippet"] is None:
            try:
                debug["first_response_snippet"] = {"status": resp.status_code, "snippet": resp.text[:1000]}
            except Exception:
                debug["first_response_snippet"] = {"status": resp.status_code, "snippet": None}

        if resp.status_code == 404:
            # often indicates wrong method/path or missing permission — stop and return debug
            debug["errors"].append(f"Upstream 404 on page {page} (method=POST). URL: {resp.url}")
            break
        if resp.status_code >= 400:
            debug["errors"].append(f"Upstream non-200 on page {page}: {resp.status_code}")
            # try to include body
            try:
                debug["errors"].append(resp.text[:1000])
            except Exception:
                pass
            break

        try:
            data = resp.json()
        except Exception:
            debug["errors"].append("Upstream returned non-JSON or invalid JSON")
            break

        # search endpoint shapes vary: try to find results in several places
        results = []
        if isinstance(data, dict):
            # many responses include "results" or "matches" or "hits"
            if "results" in data and isinstance(data["results"], list):
                results = data["results"]
            elif "matches" in data and isinstance(data["matches"], list):
                results = data["matches"]
            elif "hits" in data and isinstance(data["hits"], list):
                results = data["hits"]
            else:
                # some OpenSanctions responses return top-level list inside 'data' or similar nested structure
                # try to find the first list value
                for k, v in data.items():
                    if isinstance(v, list):
                        results = v
                        break
        elif isinstance(data, list):
            results = data

        # if no results: stop paging (either true empty or endpoint doesn't support wildcard)
        if not results:
            # if this was the very first page, include debug snippet
            if fetched == 0:
                debug["errors"].append("No results returned from upstream (empty results list). Check query or endpoint access.")
            break

        # process results
        for r in results:
            fetched += 1
            # attempt to coerce into normalized record fields
            name = extract_name(r)
            nationality = extract_nationality(r) or "Unknown"
            # update totals by nationality
            totals[nationality] = totals.get(nationality, 0) + 1
            # add sample names per nationality (limit)
            samples.setdefault(nationality, [])
            if len(samples[nationality]) < sample_per_country:
                samples[nationality].append(name or "—")

            # datasets
            ds_list = extract_datasets(r)
            for ds in ds_list:
                datasets_count[ds] = datasets_count.get(ds, 0) + 1

        # small polite pause to avoid rate limits if user requested many pages
        time.sleep(0.15)

    meta = {
        "fetched": fetched,
        "pages": min(max_pages, page),
        "max_pages": max_pages,
        "used_url": used_url
    }

    return {"totals": totals, "datasets": datasets_count, "samples": samples, "meta": meta, "debug": debug}
