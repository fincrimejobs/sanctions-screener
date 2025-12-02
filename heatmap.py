# heatmap.py
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import os, requests, time
from collections import Counter, defaultdict
from typing import Optional, Dict, Any

router = APIRouter()

# in-process cache (per process). Use Redis for multi-instance production.
_HEATMAP_CACHE = {"ts": 0, "data": None}
CACHE_TTL = 60 * 60  # 1 hour, adjust as needed

def _find_countries_in_entity(ent: Dict[str, Any]):
    found = []
    if not ent or not isinstance(ent, dict):
        return found
    identity = ent.get("identity") or ent.get("properties") or ent
    for key in ("nationality", "country", "countries", "citizenship"):
        v = identity.get(key) if isinstance(identity, dict) else None
        if v:
            if isinstance(v, list):
                found.extend([str(x) for x in v if x])
            else:
                found.append(str(v))
    addrs = None
    if isinstance(identity, dict):
        addrs = identity.get("addresses")
    if addrs is None:
        addrs = ent.get("addresses")
    if addrs and isinstance(addrs, list):
        for a in addrs:
            if isinstance(a, dict):
                for k in ("country", "country_code", "country_name"):
                    if a.get(k):
                        found.append(str(a.get(k)))
    # lightweight fallback crawl (avoid huge strings)
    for k,v in (ent.items() if isinstance(ent, dict) else []):
        if isinstance(v, str) and 2 <= len(v) <= 60:
            tok = v.strip()
            if any(ch.isalpha() for ch in tok):
                found.append(tok)
    # normalize & dedupe
    clean = []
    for f in found:
        s = " ".join(str(f).split())
        if s and s not in clean:
            clean.append(s)
    return clean

@router.get("/heatmap")
def heatmap(country_field: Optional[str] = "auto", max_pages: int = 8, per_page: int = 100):
    """
    Aggregates OpenSanctions entities into counts per country.
    - country_field: "auto" or a specific field name to prefer.
    - max_pages: pages to fetch (safe default 8).
    - per_page: page size (default 100).
    Returns JSON: { totals: {country:count}, samples: {country: [...]}, meta: {...} }
    """
    now = time.time()
    # return cache if fresh
    if _HEATMAP_CACHE["data"] and now - _HEATMAP_CACHE["ts"] < CACHE_TTL:
        return JSONResponse(content=_HEATMAP_CACHE["data"])

    api_key = os.getenv("OPENSANCTIONS_KEY")
    if not api_key:
        return JSONResponse(content={"error": "OPENSANCTIONS_KEY not set"}, status_code=500)

    base = "https://api.opensanctions.org/entities"
    headers = {"Accept": "application/json"}
    params = {"limit": per_page, "api_key": api_key}

    country_counts = Counter()
    country_samples = defaultdict(list)
    total_fetched = 0
    last_page_used = 0

    for page in range(1, max_pages + 1):
        params["page"] = page
        last_page_used = page
        try:
            r = requests.get(base, params=params, headers=headers, timeout=20)
            if r.status_code != 200:
                break
            payload = r.json()
        except Exception:
            break

        items = []
        if isinstance(payload, dict):
            if payload.get("results"):
                items = payload.get("results") or []
            elif payload.get("entities"):
                items = payload.get("entities") or []
            else:
                for k in ("data", "items", "rows"):
                    if payload.get(k):
                        items = payload.get(k)
                        break
        elif isinstance(payload, list):
            items = payload

        if not items:
            break

        for ent in items:
            total_fetched += 1
            countries = []
            if country_field != "auto":
                v = ent.get(country_field) or (ent.get("identity") or {}).get(country_field)
                if v:
                    countries = [v] if isinstance(v, str) else list(v)
            else:
                countries = _find_countries_in_entity(ent)

            for c in countries:
                cstr = str(c).strip()
                if not cstr:
                    continue
                if 2 <= len(cstr) < 60:
                    country_counts[cstr] += 1
                    if len(country_samples[cstr]) < 5:
                        sample = {
                            "name": ent.get("name") or ent.get("caption") or ent.get("title") or "",
                            "datasets": ent.get("datasets") or ent.get("dataset") or [],
                            "raw_id": ent.get("id") or ent.get("@id"),
                            "source": None
                        }
                        s = ent.get("sources")
                        if s:
                            if isinstance(s, list) and s:
                                if isinstance(s[0], str):
                                    sample["source"] = s[0]
                                elif isinstance(s[0], dict):
                                    sample["source"] = s[0].get("url") or s[0].get("link") or s[0].get("id")
                        country_samples[cstr].append(sample)

        if isinstance(payload, dict) and not payload.get("next") and (len(items) < per_page):
            break

    top = country_counts.most_common(50)
    result = {
        "totals": dict(top),
        "samples": {k: v for k, v in list(country_samples.items())},
        "meta": {"fetched": total_fetched, "pages": last_page_used, "max_pages": max_pages}
    }

    _HEATMAP_CACHE["ts"] = now
    _HEATMAP_CACHE["data"] = result
    return JSONResponse(content=result)
