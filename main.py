from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
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

class BatchInput(BaseModel):
    names: List[str]

@app.get("/")
def home():
    return {"message": "Screener API is running!"}

@app.post("/screen")
def screen_person(item: ScreenerInput):
    # Fallback logic
    api_key = os.getenv("BULK_API_KEY") or os.getenv("OPENSANCTIONS_KEY", "")
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    payload = {"queries": {"q1": {"schema": "Person", "properties": {"name": [item.name]}}}}
    
    try:
        resp = requests.post(url, json=payload)
        data = resp.json()
        results = data.get("responses", {}).get("q1", {}).get("results", [])
        
        hits = []
        for result in results:
            if result['score'] >= 0.6: 
                hits.append({
                    "name": result['caption'],
                    "score": int(result['score'] * 100),
                    "datasets": ", ".join(result['datasets']),
                    "birth_date": result['properties'].get("birthDate", ["Unknown"])[0],
                    "nationality": result['properties'].get("nationality", ["Unknown"])[0]
                })
        return {"status": "hit" if hits else "clean", "matches": hits}
    except Exception as e:
        return {"status": "error", "error_details": str(e)}

@app.post("/batch")
def batch_screen(item: BatchInput):
    # 1. Grab Keys
    bulk_key = os.getenv("BULK_API_KEY")
    std_key = os.getenv("OPENSANCTIONS_KEY")
    
    # 2. DEBUG INFO: Return this if it fails
    debug_info = {
        "key_used": "BULK" if bulk_key else ("STANDARD" if std_key else "NONE"),
        "key_length": len(bulk_key) if bulk_key else 0
    }

    api_key = bulk_key or std_key or ""
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    queries = {}
    for index, name in enumerate(item.names):
        if name.strip():
            queries[f"row_{index}"] = {"schema": "Person", "properties": {"name": [name]}}

    if not queries:
        return {"error": "No valid names provided"}

    try:
        response = requests.post(url, json={"queries": queries})
        
        # --- DIAGNOSTIC BLOCK ---
        # If the external API fails, we return the EXACT message they sent us
        if response.status_code != 200:
            return {
                "batch_results": [],
                "ERROR_TYPE": "External API Failed",
                "STATUS_CODE": response.status_code,
                "SERVER_RESPONSE": response.text, 
                "DEBUG_INFO": debug_info
            }
        # ------------------------

        data = response.json().get("responses", {})
        final_output = []
        
        for index, name in enumerate(item.names):
            query_id = f"row_{index}"
            results = data.get(query_id, {}).get("results", [])
            
            best_match = None
            if results and results[0]['score'] >= 0.6:
                r = results[0]
                best_match = {
                    "match_name": r['caption'],
                    "score": int(r['score'] * 100),
                    "lists": ", ".join(r['datasets'][:3]),
                    "country": r['properties'].get("nationality", ["Unknown"])[0]
                }
            
            final_output.append({
                "status": "hit" if best_match else "clean",
                "data": best_match if best_match else {}
            })

        return {"batch_results": final_output}

    except Exception as e:
        # If Python crashes, return the crash report
        return {
            "batch_results": [],
            "ERROR_TYPE": "Internal Python Crash",
            "DETAILS": str(e),
            "DEBUG_INFO": debug_info
        }