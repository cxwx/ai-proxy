from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
import asyncio
import json
import os

app = FastAPI()

def must_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value

CHERRY_URL = must_env("CHERRY_URL")
TARGET_API_KEY = must_env("CHERRY_API_KEY")
CHERRY_MODEL = must_env("CHERRY_MODEL")

@app.post("/v1/chat/completions")
async def chat_completions(req: Request):
    client_payload = await req.json()
    
    client_payload["model"] = CHERRY_MODEL

    async def stream_generator():
        async with httpx.AsyncClient(timeout=None) as client:
            headers = {
                "Authorization": f"Bearer {TARGET_API_KEY}",
                "Content-Type": "application/json"
            }
            async with client.stream("POST", CHERRY_URL, json=client_payload, headers=headers) as resp:
                content = ""
                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data: "):
                        continue
                    data = line[len("data: "):]
                    if data == "[DONE]":
                        yield "data: [DONE]\n\n"
                        return
                    try:
                        chunk = json.loads(data)
                        # print(chunk)
                    except json.JSONDecodeError:
                        continue
                    yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                    await asyncio.sleep(0)
    return StreamingResponse(stream_generator(), media_type="text/event-stream")


