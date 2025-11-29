from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os
from typing import List

app = FastAPI()

# Allow Webflow to talk to this code (for production prefer limiting origins)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ScreenerInput(BaseModel):
    name: str

@app.get("/")
def home():
    return {"message": "Screener API is running!"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    api_key = os.getenv("OPENSANCTIONS_KEY")
    if not api_key:
        # Helpful error for debugging
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
        # Forward upstream body for easier debugging
        raise HTTPException(status_code=502, detail=f"Upstream returned {resp.status_code}: {resp.text[:1000]}")

    try:
        data = resp.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Upstream returned non-JSON response")

    # Safely dig into response
    results = data.get("responses", {}).get("q1", {}).get("results", [])
    if not isinstance(results, list):
        results = []

    hits: List[dict] = []
    for result in results:
        score = result.get("score", 0)
        try:
            score = float(score)
        except Exception:
            score = 0.0

        if score > 0.7:
            hits.append({
                "name": result.get("caption"),
                "score": score,
                "datasets": result.get("datasets", [])
            })

    return {
        "status": "hit" if hits else "clean",
        "matches": hits
    }
