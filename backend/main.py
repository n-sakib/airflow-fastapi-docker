import aiohttp
import base64
from uuid import UUID
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, UUID4

app = FastAPI()

class DAGRunRequest(BaseModel):
    dag_id: str
    task_uuid: UUID

@app.get("/")
def read_root():
    return {"message": "Welcome to the Airflow FastAPI integration example"}

@app.get("/health", status_code=200)
async def health_check():
    return JSONResponse(content={"status": "ok"})

@app.post("/trigger_dag/")
async def trigger_dag(request: DAGRunRequest):
    url = f"http://airflow-webserver:8080/api/v1/dags/{request.dag_id}/dagRuns"
    headers = {"Content-Type": "application/json", "Authorization": "Basic " + base64.b64encode(b"airflow:airflow").decode("utf-8")}
    data = {"dag_run_id": str(request.task_uuid)}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:
                    response_data = await response.json()
                    print("Success:")
                    print(response_data)
                    return response_data
                else:
                    error_text = await response.text()
                    print(f"Failed with status {response.status}: {error_text}")
                    raise HTTPException(status_code=response.status, detail=error_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: An unexpected error occurred. {e}")