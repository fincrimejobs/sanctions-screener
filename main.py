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

# --- SINGLE SCREENING (Old Key) ---
@app.post("/screen")
def screen_person(item: ScreenerInput):
    api_key = os.getenv("OPENSANCTIONS_KEY", "") 
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    payload = {
        "queries": { "q1": { "schema": "Person", "properties": {"name": [item.name]} } }
    }
    
    return process_response(requests.post(url, json=payload), "q1")

# --- BULK SCREENING (New Key) ---
@app.post("/batch")
def batch_screen(item: BatchInput):
    # USE THE BULK KEY HERE
    api_key = os.getenv("BULK_API_KEY", "") 
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    # 1. Build a "Batch Payload" (many queries at once)
    queries = {}
    for index, name in enumerate(item.names):
        if name.strip():
            # We use the name itself as the ID to track it
            queries[f"row_{index}"] = {
                "schema": "Person", 
                "properties": {"name": [name]}
            }

    if not queries:
        return {"results": []}

    # 2. Send one big request
    response = requests.post(url, json={"queries": queries})
    data = response.json().get("responses", {})

    # 3. Format the results into a clean list
    final_output = []
    for key, val in data.items():
        # Get the original name back from the query logic if needed, 
        # but here we just process the results
        results = val.get("results", [])
        best_match = None
        
        # Find the highest score
        if results and results[0]['score'] > 0.7:
            r = results[0]
            best_match = {
                "match_name": r['caption'],
                "score": int(r['score'] * 100),
                "lists": ", ".join(r['datasets'][:3]), # First 3 lists
                "country": r['properties'].get("nationality", ["Unknown"])[0]
            }
        
        final_output.append({
            "query_id": key,
            "status": "hit" if best_match else "clean",
            "data": best_match
        })

    return {"batch_results": final_output}

# --- HELPER FUNCTION ---
def process_response(response, query_id):
    data = response.json()
    results = data.get("responses", {}).get(query_id, {}).get("results", [])
    
    hits = []
    for result in results:
        if result['score'] > 0.7: 
            props = result['properties']
            hits.append({
                "name": result['caption'],
                "score": int(result['score'] * 100),
                "datasets": ", ".join(result['datasets']),
                "birth_date": props.get("birthDate", ["Unknown"])[0],
                "nationality": props.get("nationality", ["Unknown"])[0],
                "topics": ", ".join(props.get("topics", []))
            })
            
    return {"status": "hit" if hits else "clean", "matches": hits}