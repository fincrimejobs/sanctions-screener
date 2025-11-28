from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os

app = FastAPI()

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
    return {"message": "Single Screener API is Live"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    # Use the standard key. If missing, it might work on free tier (rate limited)
    api_key = os.getenv("OPENSANCTIONS_KEY", "")
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    # Simple Query
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        }
    }
    
    try:
        resp = requests.post(url, json=payload)
        
        # Safety: Check if API failed
        if resp.status_code != 200:
            return {"status": "error", "message": "External API Error"}

        data = resp.json()
        results = data.get("responses", {}).get("q1", {}).get("results", [])
        
        hits = []
        for result in results:
            # 60% Match Threshold
            if result['score'] >= 0.6: 
                props = result['properties']
                hits.append({
                    "name": result['caption'],
                    "score": int(result['score'] * 100),
                    "datasets": ", ".join(result['datasets']),
                    "birth_date": props.get("birthDate", ["Unknown"])[0],
                    "nationality": props.get("nationality", ["Unknown"])[0]
                })
                
        return {"status": "hit" if hits else "clean", "matches": hits}

    except Exception as e:
        return {"status": "error", "message": str(e)}