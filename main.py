from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import requests
import os

app = FastAPI()

# --- SECURITY ---
origins = ["*"] # Allow all for now to ensure connection

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "Universal API is Live"}

@app.post("/screen")
async def screen_person(request: Request):
    # 1. READ RAW DATA (Bypasses 422 Errors)
    try:
        body = await request.json()
    except:
        return {"status": "error", "message": "Invalid JSON format"}

    # 2. FIGURE OUT WHAT WE GOT
    # Did we get {"name": "Putin"} or {"names": ["Putin"]}?
    search_term = ""
    
    if "name" in body:
        search_term = body["name"]
    elif "names" in body and isinstance(body["names"], list):
        search_term = body["names"][0] # Take the first one
    else:
        return {"status": "error", "message": f"Server received unexpected data: {body}"}

    # 3. RUN SEARCH
    api_key = os.getenv("OPENSANCTIONS_KEY", "").strip()
    url = "https://api.opensanctions.org/match/default"
    headers = {"Authorization": f"ApiKey {api_key}"}
    
    payload = {
        "queries": {
            "q1": {
                "schema": "Person",
                "properties": {"name": [search_term]}
            }
        }
    }
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        
        if resp.status_code != 200:
            return {"status": "error", "message": f"Sanctions API Error: {resp.status_code}"}

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