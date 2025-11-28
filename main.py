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
    return {"message": "Screener API is running!"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    # Use the default key if not set in env
    api_key = os.getenv("OPENSANCTIONS_KEY", "") 
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [item.name]}
            }
        }
    }
    
    response = requests.post(url, json=payload)
    data = response.json()
    results = data.get("responses", {}).get("q1", {}).get("results", [])
    
    hits = []
    for result in results:
        # 0.7 means 70% match confidence
        if result['score'] > 0.7: 
            props = result['properties']
            
            # Extract professional details
            hit_data = {
                "name": result['caption'],
                "score": int(result['score'] * 100), # Convert to percentage (e.g., 95)
                "datasets": ", ".join(result['datasets']), # List of sanctions lists
                "birth_date": props.get("birthDate", ["Unknown"])[0],
                "nationality": props.get("nationality", ["Unknown"])[0],
                "topics": ", ".join(props.get("topics", [])) # e.g., "role.pep"
            }
            hits.append(hit_data)
            
    return {
        "status": "hit" if len(hits) > 0 else "clean",
        "matches": hits
    }