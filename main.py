from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import requests
import os

app = FastAPI()

# --- SECURITY: LOCK THE DOOR ---
# Update this with your actual site URL if you want strict security
origins = ["*"] 

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# This matches your Frontend JSON: {"name": "Vladimir Putin"}
class ScreenerInput(BaseModel):
    name: str

@app.get("/")
def home():
    return {"message": "Screener API is Live"}

# --- EXCEPTION HANDLER (Fixes the 'undefined' error) ---
@app.exception_handler(422)
async def validation_exception_handler(request: Request, exc):
    return JSONResponse(
        status_code=422,
        content={"status": "error", "message": "Data Format Error. Backend expected {'name': 'text'}"},
    )

@app.post("/screen")
def screen_person(item: ScreenerInput):
    # 1. Get Key and Clean it
    api_key = os.getenv("OPENSANCTIONS_KEY", "").strip()
    
    # 2. Use Headers 
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
        resp = requests.post(url, json=payload, headers=headers)
        
        if resp.status_code != 200:
            return {"status": "error", "message": f"Sanctions Database Error: {resp.status_code}"}

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