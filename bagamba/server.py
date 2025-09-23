from fastapi import FastAPI, Request
from slack_bolt.adapter.fastapi import SlackRequestHandler
from main import app, incident_manager, duty_manager
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    incident_manager.init()
    duty_manager.init()
    yield


# FastAPI приложение
api = FastAPI(lifespan=lifespan)
handler = SlackRequestHandler(app)


@api.post("/slack/")
async def slack_events(req: Request):
    data = await req.json()
    if "challenge" in data.keys():
        return data["challenge"]
    return await handler.handle(req)


@api.post("/slack/interactive")
async def slack_interactive(req: Request):
    return await handler.handle(req)


@api.get("/health")
async def health():
    return {"status": "ok"}
