# heatmap.py
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import os
import requests
import math
import logging
import time
import itertools
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("heatmap")

router = APIRouter()

# Small utility to recursively find keys (reuse same logic as main.py, robust)
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

# Extract a "country" from an entity record (best-effort)
def pick_country_from_entity(e: Dict[str,Any]):
    # common fields or nested patterns
    possible = []
    # direct fields
    for f in ("country", "nationality", "country_of_residence", "citizenship", "countries"):
        v = e.get(f)
        if v:
            possible.append(v)
    # identity block
    id_block = e.get("identity") or e.get("identities") or {}
    if id_block:
        v = recursive_find(id_block, ["country", "nationality", "citizenship", "country_of_residence"])
        if v:
            possible.append(v)
    # addresses or birth places
    v = recursive_find(e, ["birth_place", "place_of_birth", "born_in"])
    if v:
        possible.append(v)
    # nested addresses or locations (list of dicts)
    if isinstance(e.get("addresses"), list):
        for a in e.get("addresses")[:3]:
            if isinstance(a, dict):
                c = a.get("country") or a.get("country_code") or recursive_find(a, ["country"])
                if c:
                    possible.append(c)
    # sometimes the entity contains a 'sources' or 'datasets' token with country-like codes
    # fallback to scanning any string fields that look like an ISO country
    # normalize to string
    for p in possible:
        if isinstance(p, list) and p:
            # prefer first string in list
            for x in p:
                if isinstance(x, str) and x.strip():
                    return x.strip()
        if isinstance(p, str) and p.strip():
            return p.strip()
    return None

# Extract a short sample record to show in UI
def summarize_entity(e: Dict[str,Any]):
    name = e.get("name") or e.get("caption") or recursive_find(e, ["name", "caption"]) or None
    # find score if any
    score = e.get("score") or recursive_find(e, ["score", "confidence"]) or None
    # find any sources (normalized to strings)
    sources = []
    if e.get("sources"):
        if isinstance(e.get("sources"), list):
            sources = [str(x) for x in e.get("sources")[:6]]
        else:
            sources = [str(e.get("sources"))]
    else:
        # deep-scan some fields
        s = recursive_find(e, ["urls", "links", "sources", "url", "link"])
        if s:
            if isinstance(s, list):
                sources = [str(x) for x in s[:6]]
            else:
                sources = [str(s)]
    return {"name": name, "score": float(score) if (score is not None and str(score).strip()) else None, "sources": sources}

# Build a best-effort list of candidate "records" from an API response page
def unpack_entities_page(resp_json):
    # Look for a variety of shapes in OpenSanctions / yente / proxied services
    candidates = []
    if isinstance(resp_json, dict):
        # common keys: 'results', 'entities', 'data', 'items', 'hits'
        for k in ("results", "entities", "data", "items", "hits", "rows"):
            if k in resp_json and resp_json[k]:
                if isinstance(resp_json[k], list):
                    candidates.extend(resp_json[k])
                    break
                elif isinstance(resp_json[k], dict) and "items" in resp_json[k]:
                    candidates.extend(resp_json[k]["items"])
                    break
        # sometimes responses are directly an array under the root
        if not candidates:
            # check if the root dict itself looks like an entity (has 'id' and 'name'/'caption')
            if "id" in resp_json and ("name" in resp_json or "caption" in resp_json):
                candidates.append(resp_json)
    elif isinstance(resp_json, list):
        candidates.extend(resp_json)
    return candidates

@router.get("/heatmap")
def heatmap(max_pages: int = 8, page_size: int = 200):
    """
    Generate a country-count heatmap using OpenSanctions 'entities' pages.
    - max_pages: how many pages to attempt (default 8)
    - page_size: page size passed to API (default 200). Some APIs cap this; code will still proceed.
    """
    api_key = os.getenv("OPENSANCTIONS_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENSANCTIONS_KEY not set on server")

    base_urls_to_try = [
        "https://api.opensanctions.org/entities",
        "https://api.opensanctions.org/data/entities",
        "https://api.opensanctions.org/search",   # fallback
    ]

    totals = {}
    samples = {}
    fetched = 0
    used_url = None
    first_response_snippet = None

    # try each base URL until we get some results
    for base in base_urls_to_try:
        logger.info("Trying heatmap base URL: %s", base)
        used_url = base
        fetched = 0
        totals = {}
        samples = {}
        first_response_snippet = None

        for p in range(1, max_pages + 1):
            params = {"api_key": api_key, "page": p, "size": page_size}
            try:
                r = requests.get(base, params=params, timeout=20)
            except requests.RequestException as exc:
                logger.warning("Request to %s failed (page %s): %s", base, p, str(exc))
                # short backoff and try next page or next base
                time.sleep(0.5)
                continue

            if r.status_code == 404:
                logger.info("Endpoint %s returned 404 - skipping to next candidate", base)
                break
            if r.status_code == 401 or r.status_code == 403:
                logger.error("Auth error (%s) from %s: %s", r.status_code, base, r.text[:200])
                # auth issues are fatal for this base; don't keep trying pages
                break

            try:
                j = r.json()
            except Exception:
                logger.warning("Non-JSON response at %s page %s: %s", base, p, r.text[:200])
                if first_response_snippet is None:
                    first_response_snippet = {"status": r.status_code, "text": r.text[:1000]}
                continue

            if first_response_snippet is None:
                # keep a small snippet for debugging if final result is empty
                try:
                    first_response_snippet = {"status": r.status_code, "body_sample": (j if isinstance(j, dict) else str(j)) if len(str(j)) < 2000 else str(j)[:2000]}
                except Exception:
                    first_response_snippet = {"status": r.status_code}

            page_entities = unpack_entities_page(j)
            if not page_entities:
                # if this page returned empty list or no recognized shape, stop paging this base
                logger.info("No entities found on %s page %s (no candidate array)", base, p)
                # If first page had nothing, try next base URL
                if p == 1:
                    break
                else:
                    # continue to next page in case pages further might contain content
                    continue

            # process entities on this page
            for ent in page_entities:
                fetched += 1
                # try to determine country: look for country/nationality fields
                c = pick_country_from_entity(ent) or recursive_find(ent, ["country", "nationality", "citizenship", "country_of_residence"])
                if c:
                    # normalize simple objects into strings (if dict with 'name' inside)
                    if isinstance(c, dict) and "name" in c:
                        cstr = str(c.get("name"))
                    else:
                        cstr = str(c)
                    cstr = cstr.strip()
                    if not cstr:
                        continue
                    totals[cstr] = totals.get(cstr, 0) + 1
                    # store a sample if none yet (use summarized record)
                    if cstr not in samples and isinstance(ent, dict):
                        samples[cstr] = summarize_entity(ent)
                else:
                    # also try a looser check: sometimes 'addresses' contain country codes deep nested
                    # we will attempt to extract any string that looks like a country code (2-3 letters) or name
                    addr_country = recursive_find(ent, ["country", "country_name", "country_code"])
                    if addr_country:
                        cstr = str(addr_country).strip()
                        if cstr:
                            totals[cstr] = totals.get(cstr, 0) + 1
                            if cstr not in samples:
                                samples[cstr] = summarize_entity(ent)
                    # otherwise skip (we don't count unknown-country entities)
            # if server returned less than page_size, probably last page
            # attempt to detect an explicit 'count' to short-circuit
            total_estimate = None
            if isinstance(j, dict):
                for possible_total_key in ("total", "total_count", "count", "size"):
                    if possible_total_key in j and isinstance(j[possible_total_key], int):
                        total_estimate = j[possible_total_key]
                        break
            # if page had fewer than page_size items, we can break
            if len(page_entities) < page_size:
                logger.info("Page %s had %s items (< page_size=%s) — stopping pagination for base %s", p, len(page_entities), page_size, base)
                break
            # small throttle
            time.sleep(0.15)

        # If we found anything, stop trying other base URLs
        if fetched > 0 or totals:
            logger.info("Found %s country records using base %s", fetched, base)
            break
        else:
            logger.info("No country records found using base %s, trying next candidate base", base)
            # continue to next base url candidate

    # Prepare sorted totals & minimal meta
    sorted_totals = {k: totals[k] for k in sorted(totals, key=totals.get, reverse=True)}
    meta = {"fetched": fetched, "pages": max_pages if max_pages else 0, "max_pages": max_pages, "used_url": used_url}

    # If nothing found — return helpful debug info with first_response_snippet
    if not sorted_totals:
        return {"totals": {}, "samples": {}, "meta": meta, "debug": {"first_response_snippet": first_response_snippet}}

    # pick up to 5 sample countries to show
    top_countries = list(sorted_totals.keys())[:20]
    sample_small = {c: samples.get(c) for c in top_countries[:10] if c in samples}

    return {"totals": sorted_totals, "samples": sample_small, "meta": meta}
