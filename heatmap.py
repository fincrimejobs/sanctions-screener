# heatmap.py
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import os
import requests
import time
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("heatmap")

router = APIRouter()

# Reusable recursive finder (same idea used in main.py)
def recursive_find(obj: Any, keys: List[str]):
    if obj is None:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.lower() in [kk.lower() for kk in keys]:
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

def pick_country_from_entity(e: Dict[str,Any]):
    # Try common fields and nested identity blocks
    possible = []
    for f in ("country", "nationality", "country_of_residence", "citizenship", "countries"):
        v = e.get(f)
        if v:
            possible.append(v)
    id_block = e.get("identity") or e.get("identities") or {}
    if id_block:
        v = recursive_find(id_block, ["country", "nationality", "citizenship", "country_of_residence"])
        if v:
            possible.append(v)
    v = recursive_find(e, ["birth_place", "place_of_birth", "born_in"])
    if v:
        possible.append(v)
    if isinstance(e.get("addresses"), list):
        for a in e.get("addresses")[:3]:
            if isinstance(a, dict):
                c = a.get("country") or a.get("country_code") or recursive_find(a, ["country"])
                if c:
                    possible.append(c)
    for p in possible:
        if isinstance(p, list) and p:
            for x in p:
                if isinstance(x, str) and x.strip():
                    return x.strip()
        if isinstance(p, str) and p.strip():
            return p.strip()
        if isinstance(p, dict) and p.get("name"):
            return str(p.get("name")).strip()
    return None

def summarize_entity(e: Dict[str,Any]):
    name = e.get("name") or e.get("caption") or recursive_find(e, ["name", "caption"]) or None
    score = e.get("score") or recursive_find(e, ["score", "confidence"]) or None
    sources = []
    if e.get("sources"):
        if isinstance(e.get("sources"), list):
            sources = [str(x) for x in e.get("sources")[:6]]
        else:
            sources = [str(e.get("sources"))]
    else:
        s = recursive_find(e, ["urls", "links", "sources", "url", "link"])
        if s:
            if isinstance(s, list):
                sources = [str(x) for x in s[:6]]
            else:
                sources = [str(s)]
    return {"name": name, "score": float(score) if (score is not None and str(score).strip()) else None, "sources": sources}

def unpack_entities_page(resp_json):
    candidates = []
    if isinstance(resp_json, dict):
        for k in ("results", "entities", "data", "items", "hits", "rows"):
            if k in resp_json and resp_json[k]:
                if isinstance(resp_json[k], list):
                    candidates.extend(resp_json[k])
                    break
                elif isinstance(resp_json[k], dict) and "items" in resp_json[k] and isinstance(resp_json[k]["items"], list):
                    candidates.extend(resp_json[k]["items"])
                    break
        if not candidates:
            if "id" in resp_json and ("name" in resp_json or "caption" in resp_json):
                candidates.append(resp_json)
    elif isinstance(resp_json, list):
        candidates.extend(resp_json)
    return candidates

@router.get("/heatmap")
def heatmap(max_pages: int = 8, page_size: int = 200):
    """
    Produces a country-count heatmap from OpenSanctions.
    - max_pages: how many pages to try
    - page_size: page size to request
    """
    api_key = os.getenv("OPENSANCTIONS_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENSANCTIONS_KEY not set on server")

    base_urls_to_try = [
        "https://api.opensanctions.org/entities",
        "https://api.opensanctions.org/data/entities",
        "https://api.opensanctions.org/search"   # fallback - called with q='*' now
    ]

    totals = {}
    samples = {}
    fetched = 0
    used_url = None
    first_response_snippet = None

    for base in base_urls_to_try:
        logger.info("Trying heatmap base URL: %s", base)
        used_url = base
        totals = {}
        samples = {}
        fetched = 0
        first_response_snippet = None

        for p in range(1, max_pages + 1):
            # If we are using the search endpoint, require a broad query param
            params = {"api_key": api_key, "page": p, "size": page_size}
            if base.endswith("/search"):
                # Use q=* to request a broad search result (search endpoint requires q)
                params["q"] = "*"

            try:
                r = requests.get(base, params=params, timeout=20)
            except requests.RequestException as exc:
                logger.warning("Request to %s failed (page %s): %s", base, p, str(exc))
                time.sleep(0.5)
                continue

            # Always capture a snippet (text or JSON) for debugging if nothing is found later
            snippet = None
            try:
                j = r.json()
                # small pretty snippet won't exceed large payloads
                snippet = j if isinstance(j, dict) and len(str(j)) < 4000 else str(j)[:2000]
            except Exception:
                snippet = r.text[:2000] if r.text else f"status:{r.status_code}"

            if first_response_snippet is None:
                first_response_snippet = {"status": r.status_code, "snippet": snippet}

            if r.status_code == 404:
                logger.info("Endpoint %s returned 404 - skipping to next candidate", base)
                break
            if r.status_code in (401, 403):
                logger.error("Auth error (%s) from %s: %s", r.status_code, base, (r.text[:200] if r.text else ""))
                break

            # attempt to parse JSON into page_entities
            page_entities = []
            try:
                j = r.json()
                page_entities = unpack_entities_page(j)
            except Exception:
                logger.warning("Non-JSON response at %s page %s: %s", base, p, r.text[:200])
                page_entities = []

            if not page_entities:
                logger.info("No entities found on %s page %s (no candidate array)", base, p)
                # if this was the search fallback and it returned empty on p=1, try next base
                if p == 1:
                    break
                else:
                    # continue to next page attempt
                    continue

            # process entities
            for ent in page_entities:
                fetched += 1
                c = pick_country_from_entity(ent) or recursive_find(ent, ["country", "nationality", "citizenship", "country_of_residence"])
                if c:
                    if isinstance(c, dict) and "name" in c:
                        cstr = str(c.get("name"))
                    else:
                        cstr = str(c)
                    cstr = cstr.strip()
                    if not cstr:
                        continue
                    totals[cstr] = totals.get(cstr, 0) + 1
                    if cstr not in samples and isinstance(ent, dict):
                        samples[cstr] = summarize_entity(ent)
                else:
                    addr_country = recursive_find(ent, ["country", "country_name", "country_code"])
                    if addr_country:
                        cstr = str(addr_country).strip()
                        if cstr:
                            totals[cstr] = totals.get(cstr, 0) + 1
                            if cstr not in samples:
                                samples[cstr] = summarize_entity(ent)
            # if fewer than page_size items, probably last page
            if len(page_entities) < page_size:
                logger.info("Page %s had %s items (< page_size=%s) — stopping pagination for base %s", p, len(page_entities), page_size, base)
                break
            time.sleep(0.12)

        if fetched > 0 or totals:
            logger.info("Found %s country records using base %s", fetched, base)
            break
        else:
            logger.info("No country records found using base %s — trying next candidate", base)
            continue

    sorted_totals = {k: totals[k] for k in sorted(totals, key=totals.get, reverse=True)}
    meta = {"fetched": fetched, "pages": max_pages, "max_pages": max_pages, "used_url": used_url}

    if not sorted_totals:
        return {"totals": {}, "samples": {}, "meta": meta, "debug": {"first_response_snippet": first_response_snippet}}

    top_countries = list(sorted_totals.keys())[:20]
    sample_small = {c: samples.get(c) for c in top_countries[:10] if c in samples}

    return {"totals": sorted_totals, "samples": sample_small, "meta": meta}
