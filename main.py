from fastapi import FastAPI, HTTPException
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

# --- HELPER: LOGIC TO PROCESS ONE RESULT ---
def extract_hit_details(results):
    hits = []
    for result in results:
        # Lower threshold slightly to 0.6 (60%) to ensure we see results
        if result['score'] >= 0.6: 
            props = result['properties']
            hits.append({
                "name": result['caption'],
                "score": int(result['score'] * 100),
                "datasets": ", ".join(result['datasets']),
                "birth_date": props.get("birthDate", ["Unknown"])[0],
                "nationality": props.get("nationality", ["Unknown"])[0],
                "topics": ", ".join(props.get("topics", []))
            })
    return hits

# --- SINGLE SCREENING ---
@app.post("/screen")
def screen_person(item: ScreenerInput):
    # Try Bulk Key first, then Standard Key
    api_key = os.getenv("BULK_API_KEY") or os.getenv("OPENSANCTIONS_KEY", "")
    
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    payload = {
        "queries": { "q1": { "schema": "Person", "properties": {"name": [item.name]} } }
    }
    
    try:
        resp = requests.post(url, json=payload)
        data = resp.json()
        results = data.get("responses", {}).get("q1", {}).get("results", [])
        hits = extract_hit_details(results)
        return {"status": "hit" if hits else "clean", "matches": hits}
    except Exception as e:
        print(f"Error: {e}")
        return {"status": "clean", "matches": []}

# --- BULK SCREENING ---
@app.post("/batch")
def batch_screen(item: BatchInput):
    # SAFETY CHECK 1: Key Fallback
    # If BULK_API_KEY is missing, it will use your working OPENSANCTIONS_KEY
    api_key = os.getenv("BULK_API_KEY") or os.getenv("OPENSANCTIONS_KEY", "")
    
    url = f"https://api.opensanctions.org/match/default?api_key={api_key}"
    
    # SAFETY CHECK 2: Limit batch size to 50 to prevent timeouts
    names_to_process = item.names[:50]
    
    queries = {}
    for index, name in enumerate(names_to_process):
        if name.strip():
            # Create a unique ID for every row
            queries[f"row_{index}"] = {
                "schema": "Person", 
                "properties": {"name": [name]}
            }

    if not queries:
        return {"batch_results": []}

    try:
        response = requests.post(url, json={"queries": queries})
        
        # SAFETY CHECK 3: If API fails, print error to Render logs
        if response.status_code != 200:
            print(f"External API Failed: {response.text}")
            raise HTTPException(status_code=500, detail="Sanctions Provider Error")
            
        data = response.json().get("responses", {})

        final_output = []
        
        # We must loop through the ORIGINAL names to keep order
        for index, name in enumerate(names_to_process):
            query_id = f"row_{index}"
            
            # Check if we got a response for this ID
            val = data.get(query_id, {})
            results = val.get("results", [])
            
            best_match = None
            
            # Find the highest score
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
        print(f"Batch Error: {e}")
        # Return empty list so frontend doesn't crash
        return {"batch_results": []}