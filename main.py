from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os

app = FastAPI()

# 1. SECURITY: Allow everyone (Fixes CORS)
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. INPUT: Expects {"name": "Putin"}
class ScreenerInput(BaseModel):
    name: str

@app.get("/")
def home():
    return {"message": "Simple Screener is Live"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    # 3. SETUP
    api_key = os.getenv("OPENSANCTIONS_KEY", "").strip()
    url = "https://api.opensanctions.org/match/default"
    headers = {"Authorization": f"ApiKey {api_key}"}
    
    # 4. QUERY
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        }
    }
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        data = resp.json()
        results = data.get("responses", {}).get("q1", {}).get("results", [])
        
        # 5. LOGIC: Is there a match > 60%?
        hits = []
        for result in results:
            if result['score'] >= 0.6:
                hits.append({
                    "name": result['caption'],
                    "score": int(result['score'] * 100)
                })
        
        return {"status": "hit" if hits else "clean", "matches": hits}

    except Exception as e:
        return {"status": "error", "message": str(e)}