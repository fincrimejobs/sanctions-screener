from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os

app = FastAPI()

# 1. SECURITY: Allow your specific domain (and Webflow)
origins = [
    "https://fincrimejobs.webflow.io",
    "https://www.fincrimejobs.in",
    "https://fincrimejobs.in",
    "*" # Keep wildcard temporarily if you are still debugging CORS
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. STRICT INPUT MODEL
# This strictly enforces: "Give me a 'name' (text), NOT a list."
class ScreenerInput(BaseModel):
    name: str 

@app.get("/")
def home():
    return {"message": "Single Screener API is Live"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    # 3. GET KEY
    api_key = os.getenv("OPENSANCTIONS_KEY", "").strip()
    
    # 4. PREPARE SINGLE QUERY
    # We wrap the single name into the OpenSanctions format
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        }
    }
    
    url = "https://api.opensanctions.org/match/default"
    headers = {"Authorization": f"ApiKey {api_key}"}
    
    try:
        # 5. SEND TO EXTERNAL API
        resp = requests.post(url, json=payload, headers=headers)
        
        if resp.status_code != 200:
            return {"status": "error", "message": f"External API Error: {resp.status_code}"}

        data = resp.json()
        results = data.get("responses", {}).get("q1", {}).get("results", [])
        
        # 6. PROCESS RESULTS
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