# heatmap.py
from fastapi import APIRouter, HTTPException, Request
from typing import Any, Dict, List
import os
import requests
import logging
from collections import Counter, defaultdict

router = APIRouter()
logger = logging.getLogger("heatmap")
logger.setLevel(logging.INFO)

OPENSANCTIONS_KEY = os.getenv("OPENSANCTIONS_KEY")

# Candidate endpoints (we try each with an appropriate HTTP method)
CANDIDATE_ENDPOINTS = [
    {"url": "https://api.opensanctions.org/search", "method": "POST"},
    {"url": "https://api.opensanctions.org/entities", "method": "GET"},
    {"url": "https://api.opensanctions.org/datasets", "method": "GET"},
    {"url": "https://api.opensanctions.org/match/default", "method": "POST"},
]

def _auth_params_or_headers():
    """
    Return (params, headers) pair to attach to requests.
    We'll include api_key as query param if present, and also put a Token
    header if the key might be accepted that way.
    """
    params = {}
    headers = {"Accept": "application/json"}
    if OPENSANCTIONS_KEY:
        params["api_key"] = OPENSANCTIONS_KEY
        # also include Authorization header as 'Token <key>' in case server requires it
        headers["Authorization"] = f"Token {OPENSANCTIONS_KEY}"
    return params, headers

def _safe_json_snippet(resp):
    try:
        j = resp.json()
        # return small snippet to avoid large payloads
        return {"status": resp.status_code, "snippet": j if isinstance(j, dict) else j}
    except Exception:
        text = resp.text[:1000] if hasattr(resp, "text") else None
        return {"status": getattr(resp, "status_code", None), "snippet": text}

def _collect_candidates_from_payload(payload) -> List[Dict[str,Any]]:
    """
    Given JSON payload from API, try to extract a list of entity-like dicts.
    This is defensive: OpenSanctions responses vary.
    """
    if not payload:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        # common keys
        for k in ("results", "matches", "responses", "entities", "hits", "data"):
            if k in payload and isinstance(payload[k], (list, tuple)):
                return payload[k]
        # sometimes under responses->q1->results
        if "responses" in payload and isinstance(payload["responses"], dict):
            for q,v in payload["responses"].items():
                if isinstance(v, dict) and isinstance(v.get("results"), list):
                    return v.get("results")
        # fallback: top-level dict with items
        return []
    return []

def _extract_nationalities_from_entity(e) -> List[str]:
    """Try a few keys for nationality/country fields."""
    keys = ["nationality", "country", "citizenship", "country_of_residence", "citizenships"]
    out = []
    if not isinstance(e, dict):
        return out
    for k in keys:
        v = e.get(k)
        if not v:
            # try nested identity objects
            idv = e.get("identity") or e.get("entity")
            if isinstance(idv, dict) and k in idv:
                v = idv.get(k)
        if v:
            if isinstance(v, list):
                out.extend([str(x) for x in v if x])
            else:
                out.append(str(v))
    # dedupe & cleanup
    return [x.strip() for x in dict.fromkeys(out) if x and str(x).strip()]

@router.get("/heatmap")
def heatmap(request: Request, max_pages: int = 2, page_size: int = 100):
    """
    Defensive Heatmap endpoint.

    Query params:
      - max_pages: how many pages to attempt (per upstream endpoint)
      - page_size: page size for search-like endpoints

    Returns a JSON with:
      - totals: aggregated counts (example: nationality -> count)
      - samples: a few sample records
      - meta: what upstream endpoint was used, pages fetched, etc
      - debug: list of attempted endpoints & their first response snippets
    """
    if not OPENSANCTIONS_KEY:
        raise HTTPException(status_code=500, detail="Server misconfigured: OPENSANCTIONS_KEY not set")

    params_template, headers_template = _auth_params_or_headers()

    debug = {"attempts": []}
    totals = defaultdict(int)
    samples = []
    datasets = defaultdict(int)

    fetched = 0
    used_url = None

    # Try candidate endpoints until we get some entities
    for cand in CANDIDATE_ENDPOINTS:
        url = cand["url"]
        method = cand["method"].upper()
        debug_entry = {"url": url, "method": method, "pages_attempted": 0, "status_first": None, "error": None}
        logger.info("Heatmap trying %s %s", method, url)

        # attempt up to max_pages
        got_any = False
        for page in range(1, max_pages + 1):
            try:
                params = dict(params_template) if params_template else {}
                headers = dict(headers_template) if headers_template else {}
                resp = None

                if "search" in url or "match" in url:
                    # send POST body for search/match endpoints
                    if "search" in url:
                        body = {"q": "*", "page": page, "size": page_size}
                    else:
                        # match endpoint expects queries structure — try a very generic query
                        body = {"queries": {"q1": {"schema": "Person", "limit": page_size, "properties": {"name": ["*"]}}}} 
                    resp = requests.post(url, params=params, headers=headers, json=body, timeout=30)
                else:
                    # GET endpoints (entities, datasets)
                    p = dict(params)
                    p.update({"page": page, "size": page_size})
                    resp = requests.get(url, params=p, headers=headers, timeout=30)

                debug_entry["pages_attempted"] += 1
                snippet = _safe_json_snippet(resp)
                if debug_entry["status_first"] is None:
                    debug_entry["status_first"] = snippet

                # If upstream returned 404/401/403 — capture and break from paging for this endpoint
                if resp.status_code == 404:
                    debug_entry["error"] = f"Upstream 404 on page {page}"
                    logger.warning("Upstream 404 on %s page %s", url, page)
                    break
                if resp.status_code in (401, 403):
                    debug_entry["error"] = f"Upstream auth error {resp.status_code} on page {page}"
                    logger.warning("Upstream auth error %s", resp.status_code)
                    break
                if resp.status_code >= 400:
                    # record and break this endpoint (to try next candidate)
                    debug_entry["error"] = f"Upstream returned {resp.status_code} / {resp.text[:200]}"
                    logger.warning("Upstream returned error %s %s", resp.status_code, url)
                    break

                # Parse JSON & extract candidate results
                j = None
                try:
                    j = resp.json()
                except Exception:
                    debug_entry["error"] = f"Upstream returned non-JSON (status {resp.status_code})"
                    logger.warning("Non-JSON from upstream %s", url)
                    break

                # collect candidates & aggregate
                items = _collect_candidates_from_payload(j)
                # if _collect_candidates... returned empty, try heuristic: if dict has 'results' or 'matches' keys but empty, accept []
                if items:
                    got_any = True
                    used_url = url
                    for item in items:
                        fetched += 1
                        # identity/nationality heuristics
                        nats = _extract_nationalities_from_entity(item)
                        if nats:
                            for n in nats:
                                totals[n] += 1
                        # dataset counts
                        ds = item.get("datasets") or item.get("dataset") or []
                        if isinstance(ds, str):
                            datasets[ds] += 1
                        elif isinstance(ds, list):
                            for d in ds:
                                datasets[str(d)] += 1
                        # collect up to 20 samples
                        if len(samples) < 20:
                            # store small sanitized sample
                            samples.append({
                                "name": item.get("name") or item.get("caption") or None,
                                "score": item.get("score"),
                                "datasets": item.get("datasets"),
                                "sources": item.get("sources") or item.get("urls") or None,
                                "raw": (item.get("id") or item.get("qid") or None)
                            })
                    # continue paging until max_pages unless it's a search endpoint that returns empty
                else:
                    # If payload contained something but no items, and we got a 200, keep paging (maybe later pages have items).
                    # For many APIs wildcard search returns empty; we tolerate and continue to next page.
                    logger.info("No items found on %s page %s", url, page)
                # If we've collected a lot, stop early
                if fetched >= (max_pages * page_size):
                    break

            except requests.RequestException as ex:
                debug_entry["error"] = str(ex)
                logger.exception("Request to %s failed", url)
                break

        debug["attempts"].append(debug_entry)
        # if we got anything from this candidate, stop trying other endpoints
        if got_any:
            break

    # Prepare return structure
    totals_sorted = dict(sorted(totals.items(), key=lambda x: -x[1]))
    datasets_sorted = dict(sorted(datasets.items(), key=lambda x: -x[1]))

    return {
        "totals": totals_sorted,
        "datasets": datasets_sorted,
        "samples": samples,
        "meta": {
            "fetched": fetched,
            "pages": max_pages,
            "max_pages": max_pages,
            "used_url": used_url
        },
        "debug": debug
    }
