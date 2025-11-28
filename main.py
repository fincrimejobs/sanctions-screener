from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os

app = FastAPI()

# Allow Webflow to talk to this code
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all websites to connect
    allow_methods=["*"],
    allow_headers=["*"],
)

# The data we expect from Webflow
class ScreenerInput(BaseModel):
    name: str

@app.get("/")
def home():
    return {"message": "Screener API is running!"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    api_key = os.getenv("OPENSANCTIONS_KEY")
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    # Prepare data for OpenSanctions
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        }
    }
    
    # Ask OpenSanctions
    response = requests.post(url, json=payload)
    data = response.json()
    
    # Process results
    results = data.get("responses", {}).get("q1", {}).get("results", [])
    
    # Simple logic: If we have results with high score, it's a hit
    hits = []
    for result in results:
        if result['score'] > 0.7: # 70% match threshold
            hits.append({
                "name": result['caption'],
                "score": result['score'],
                "datasets": result['datasets']
            })
            
    return {
        "status": "hit" if len(hits) > 0 else "clean",
        "matches": hits
    }