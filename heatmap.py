# heatmap.py
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List, Iterable, Tuple
from collections import Counter, defaultdict
import os
import requests
import json
import time
import logging

logger = logging.getLogger("heatmap")
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/heatmap", tags=["heatmap"])

# In-memory cache so we don't hammer OpenSanctions every page load
_HEATMAP_CACHE: Dict[str, Any] = {
    "data": None,
    "updated_at": 0.0,
}

CACHE_TTL_SECONDS = 60 * 60 * 6  # 6 hours by default (override via HEATMAP_CACHE_TTL)


def _get_cache_ttl() -> int:
    try:
        return int(os.getenv("HEATMAP_CACHE_TTL", str(CACHE_TTL_SECONDS)))
    except Exception:
        return CACHE_TTL_SECONDS


def get_delivery_token() -> str:
    """
    Look up the bulk data delivery token from environment.
    We NEVER expose this token to the frontend.
    """
    token = (
        os.getenv("OPENSANCTIONS_DELIVERY_TOKEN")
        or os.getenv("OPENSANCTIONS_BULK_TOKEN")
        or os.getenv("OPEN_SANCTIONS_DELIVERY_TOKEN")
    )
    if not token:
        raise HTTPException(
            status_code=500,
            detail=(
                "Bulk data token not configured. "
                "Set OPENSANCTIONS_DELIVERY_TOKEN (or OPENSANCTIONS_BULK_TOKEN)."
            ),
        )
    return token.strip()


# --- Helpers ---------------------------------------------------------------

def ensure_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


# Minimal country normalisation: simple + cheap
_COUNTRY_MAP = {
    "US": "United States",
    "USA": "United States",
    "UNITED STATES": "United States",
    "UNITED STATES OF AMERICA": "United States",
    "RU": "Russia",
    "RUS": "Russia",
    "RUSSIAN FEDERATION": "Russia",
    "IR": "Iran",
    "IRN": "Iran",
    "IRAN, ISLAMIC REPUBLIC OF": "Iran",
    "GB": "United Kingdom",
    "UK": "United Kingdom",
    "GBR": "United Kingdom",
    "UNITED KINGDOM": "United Kingdom",
    "CN": "China",
    "CHN": "China",
    "DE": "Germany",
    "DEU": "Germany",
    "FR": "France",
    "FRA": "France",
    "CA": "Canada",
    "CAN": "Canada",
    "AU": "Australia",
    "AUS": "Australia",
    "UA": "Ukraine",
    "UKR": "Ukraine",
    "BY": "Belarus",
    "BLR": "Belarus",
    "SY": "Syria",
    "SYR": "Syria",
    "KP": "North Korea",
    "PRK": "North Korea",
    "VE": "Venezuela",
    "VEN": "Venezuela",
}


def normalize_country(raw: Any) -> List[str]:
    """
    Take a raw value (string or list) and return a list of normalized country labels.
    Kept simple & robust for marketing-level heatmap.
    """
    out: List[str] = []
    for val in ensure_list(raw):
        if not val:
            continue
        s = str(val).strip()
        if not s:
            continue

        # Split common separators: "Russia; Cyprus"
        parts = [p.strip() for p in s.replace("|", ",").replace(";", ",").split(",") if p.strip()]
        if len(parts) > 1:
            for p in parts:
                out.extend(normalize_country(p))
            continue

        u = s.upper()
        if u in _COUNTRY_MAP:
            out.append(_COUNTRY_MAP[u])
            continue

        # ISO-like 2 or 3 letter codes
        if len(u) in (2, 3) and u.isalpha():
            out.append(u)
        else:
            # Fallback: cleaned label
            cleaned = s
            if "(" in cleaned:
                cleaned = cleaned.split("(", 1)[0].strip()
            out.append(cleaned)
    return out


def extract_countries_from_entity(entity: Dict[str, Any]) -> List[str]:
    """
    From a FollowTheMoney entity record, pull out all country-like fields.
    Heuristic but works across many OpenSanctions datasets.
    """
    props: Dict[str, Any] = entity.get("properties", {}) or {}
    found: List[str] = []

    for key, value in props.items():
        lk = key.lower()
        if "country" in lk or "national" in lk or "citizen" in lk:
            found.extend(normalize_country(value))

    # Sometimes birth places or addresses embed country info
    for hint_key in ("birthPlace", "placeOfBirth", "address"):
        if hint_key in props:
            found.extend(normalize_country(props.get(hint_key)))

    # unique + non-empty
    uniq: List[str] = []
    for c in found:
        if c and c not in uniq:
            uniq.append(c)
    return uniq


def iter_dataset_names(token: str) -> Iterable[str]:
    """
    Use the bulk index.json to discover dataset names.
    Tries to be tolerant of shape changes.
    """
    index_url = f"https://data.opensanctions.org/datasets/latest/index.json?token={token}"
    logger.info("Fetching dataset index: %s", index_url)
    try:
        resp = requests.get(index_url, timeout=30)
    except requests.RequestException as e:
        logger.exception("Failed to fetch dataset index")
        raise HTTPException(status_code=502, detail=f"Failed to fetch dataset index: {e}")

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"OpenSanctions index returned {resp.status_code}: {resp.text[:500]}",
        )

    try:
        data = resp.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="OpenSanctions index returned invalid JSON")

    names: List[str] = []

    # Most likely: {"datasets": {"name": {...}, ...}}
    if isinstance(data, dict):
        if "datasets" in data and isinstance(data["datasets"], dict):
            names = list(data["datasets"].keys())
        elif "datasets" in data and isinstance(data["datasets"], list):
            for ds in data["datasets"]:
                if isinstance(ds, dict) and "name" in ds:
                    names.append(ds["name"])
        else:
            # fallback: treat top-level keys as dataset names
            for k in data.keys():
                if isinstance(k, str) and k and not k.startswith("_"):
                    names.append(k)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "name" in item:
                names.append(str(item["name"]))

    seen = set()
    for n in names:
        if not n or n in seen:
            continue
        seen.add(n)
        yield n


def choose_entities_url(dataset_name: str, token: str) -> Tuple[str, str]:
    """
    Try a small list of common entity export filenames for a dataset.
    Returns (url, filename) or (None, None).
    """
    base = f"https://data.opensanctions.org/datasets/latest/{dataset_name}"
    candidates = [
        "entities.ftm.json",
        "targets.ftm.json",
        "entities.json",
        "targets.json",
    ]
    for fname in candidates:
        url = f"{base}/{fname}?token={token}"
        try:
            head = requests.head(url, timeout=15)
        except requests.RequestException:
            continue
        if head.status_code == 200:
            return url, fname
    return None, None


def stream_entities(url: str) -> Iterable[Dict[str, Any]]:
    """
    Stream entities from a JSONL/FTM JSON export.
    Assumes one JSON object per line (typical for ftm exports).
    Keeps memory tiny for 512 MB instances.
    """
    logger.info("Streaming entities from %s", url)
    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            if resp.status_code != 200:
                logger.warning("Entity stream %s -> HTTP %s", url, resp.status_code)
                return
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                stripped = line.strip()
                # ignore array brackets if not pure JSONL
                if stripped in ("[", "]", "[{", "},", "}"):
                    continue
                try:
                    obj = json.loads(stripped)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield obj
    except requests.RequestException as e:
        logger.warning("Error streaming entities from %s: %s", url, e)
        return


def build_heatmap_full(max_entities_global: int = 1_000_000) -> Dict[str, Any]:
    """
    Core aggregation:
    - iterates all datasets visible via bulk delivery
    - streams entities, extracts countries
    - counts per country (+ per dataset)
    - stops after max_entities_global for safety
    """
    token = get_delivery_token()

    totals: Counter = Counter()
    datasets_breakdown: Dict[str, Counter] = defaultdict(Counter)
    samples: List[Dict[str, Any]] = []
    debug_attempts: List[Dict[str, Any]] = []

    processed_global = 0
    dataset_count = 0

    for ds_name in iter_dataset_names(token):
        dataset_count += 1
        ds_attempt: Dict[str, Any] = {
            "dataset": ds_name,
            "url": None,
            "entities_tried": 0,
            "entities_with_country": 0,
            "error": None,
        }

        url, fname = choose_entities_url(ds_name, token)
        ds_attempt["url"] = url
        if not url:
            ds_attempt["error"] = "No entity export found"
            debug_attempts.append(ds_attempt)
            continue

        try:
            for ent in stream_entities(url):
                ds_attempt["entities_tried"] += 1
                if processed_global >= max_entities_global:
                    break

                countries = extract_countries_from_entity(ent)
                if not countries:
                    continue

                ds_attempt["entities_with_country"] += 1
                ds_list = ensure_list(ent.get("datasets"))

                for c in countries:
                    totals[c] += 1
                    for ds in ds_list:
                        if ds:
                            datasets_breakdown[c][ds] += 1

                # collect small sample set for marketing/debug
                if len(samples) < 80:
                    props = ent.get("properties") or {}
                    name_vals = ensure_list(props.get("name"))
                    samples.append(
                        {
                            "id": ent.get("id"),
                            "name": name_vals[0] if name_vals else None,
                            "countries": countries,
                            "datasets": ds_list,
                        }
                    )

                processed_global += 1

            debug_attempts.append(ds_attempt)

            if processed_global >= max_entities_global:
                logger.info(
                    "Reached global entity cap (%s); stopping aggregation.",
                    max_entities_global,
                )
                break

        except Exception as e:
            ds_attempt["error"] = f"Exception while processing: {e}"
            debug_attempts.append(ds_attempt)
            continue

    totals_dict = dict(totals)
    datasets_dict: Dict[str, Dict[str, int]] = {
        country: dict(counter) for country, counter in datasets_breakdown.items()
    }

    return {
        "totals": totals_dict,
        "datasets": datasets_dict,
        "samples": samples,
        "meta": {
            "aggregated_countries": len(totals_dict),
            "total_entities_with_country": int(sum(totals.values())),
            "global_entity_cap": max_entities_global,
            "datasets_seen": dataset_count,
            "cached_at": int(time.time()),
        },
        "debug": {
            "attempts": debug_attempts,
        },
    }


@router.get("")
@router.get("/")
def get_heatmap(force: bool = False, cap: int = 1_000_000):
    """
    GET /heatmap or /heatmap/
      - ?force=true  -> rebuild now (ignore cache)
      - ?cap=200000  -> override global entity cap
    """
    now = time.time()
    ttl = _get_cache_ttl()
    cached = _HEATMAP_CACHE.get("data")
    updated = _HEATMAP_CACHE.get("updated_at", 0.0)

    if not force and cached is not None and (now - updated) < ttl:
        return cached

    try:
        cap_value = int(cap) if cap else 1_000_000
    except Exception:
        cap_value = 1_000_000

    data = build_heatmap_full(max_entities_global=cap_value)
    _HEATMAP_CACHE["data"] = data
    _HEATMAP_CACHE["updated_at"] = now
    return data
