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
    return {"message": "Screener API is Live"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    # 1. Get Key and TRIM whitespace (Fixes copy-paste errors)
    api_key = os.getenv("OPENSANCTIONS_KEY", "").strip()
    
    # 2. Use Headers instead of URL (More reliable)
    url = "https://api.opensanctions.org/match/default"
    headers = {"Authorization": f"ApiKey {api_key}"}
    
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        }
    }
    
    try:
        # Send request with Headers
        resp = requests.post(url, json=payload, headers=headers)
        
        # 3. DIAGNOSTIC: If it fails, return the EXACT reason
        if resp.status_code != 200:
            return {
                "status": "error", 
                "message": f"API Error {resp.status_code}: {resp.text}"
            }

        data = resp.json()
        results = data.get("responses", {}).get("q1", {}).get("results", [])
        
        hits = []
        for result in results:
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