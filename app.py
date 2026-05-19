"""Placar do II Innovathon Contagil 2026.

Painel admin web para registrar pontuacoes; placar publico atualiza em tempo
real via Server-Sent Events.

Persistencia simples em JSON no disco (data/equipes.json).

Rotas:
  GET  /                  -> placar publico (ranking ao vivo)
  GET  /admin             -> painel admin (lancar pontos)
  GET  /version           -> versao do app (auto-reload do front)
  GET  /state             -> estado atual em JSON
  GET  /stream            -> SSE com eventos do placar
  POST /admin/pontos      -> adiciona/altera pontos de uma equipe
  POST /admin/equipe      -> cria/renomeia equipe
  POST /admin/equipe/del  -> remove equipe
  POST /admin/reset       -> zera todas as pontuacoes
"""
import asyncio
import json
import os
import time
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

BASE = Path(__file__).parent
DATA_FILE = BASE / "data" / "equipes.json"
DATA_FILE.parent.mkdir(exist_ok=True)

ADMIN_PASSWORD = os.environ.get("PLACAR_ADMIN_PASSWORD", "innovathon2026")
APP_VERSION = str(int(time.time()))

# ====== Estado ======
def _carregar():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {"equipes": []}  # [{id, nome, pontos, cor_idx}]

def _salvar(estado):
    DATA_FILE.write_text(json.dumps(estado, ensure_ascii=False, indent=2), encoding="utf-8")

state = _carregar()
subscribers: list[asyncio.Queue] = []


async def broadcast(event: str, data: dict):
    payload = {"event": event, "data": data}
    for q in list(subscribers):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                subscribers.remove(q)
            except ValueError:
                pass


def _next_id():
    return max((e["id"] for e in state["equipes"]), default=0) + 1


def _next_cor():
    usadas = {e.get("cor_idx") for e in state["equipes"]}
    for i in range(1, 9):
        if i not in usadas:
            return i
    return (len(state["equipes"]) % 8) + 1


app = FastAPI(title="Innovathon 2026 - Placar")
templates = Jinja2Templates(directory=str(BASE / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")


NO_CACHE = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}


@app.get("/", response_class=HTMLResponse)
async def public(request: Request):
    return templates.TemplateResponse(
        "public.html",
        {"request": request, "app_version": APP_VERSION},
        headers=NO_CACHE,
    )


@app.get("/admin", response_class=HTMLResponse)
async def admin(request: Request):
    return templates.TemplateResponse(
        "admin.html",
        {"request": request, "app_version": APP_VERSION},
        headers=NO_CACHE,
    )


@app.get("/version")
async def version():
    return {"version": APP_VERSION}


@app.get("/state")
async def get_state():
    return state


def _check_auth(password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(401, "Senha invalida")


@app.post("/admin/equipe")
async def criar_renomear(password: str = Form(...), id: int = Form(0), nome: str = Form(...)):
    _check_auth(password)
    nome = nome.strip()
    if not nome:
        raise HTTPException(400, "Nome obrigatorio")
    if id == 0:
        nova = {"id": _next_id(), "nome": nome, "pontos": 0, "cor_idx": _next_cor()}
        state["equipes"].append(nova)
    else:
        eq = next((e for e in state["equipes"] if e["id"] == id), None)
        if not eq:
            raise HTTPException(404, "Equipe nao encontrada")
        eq["nome"] = nome
    _salvar(state)
    await broadcast("equipes", state)
    return {"ok": True}


@app.post("/admin/equipe/del")
async def remover(password: str = Form(...), id: int = Form(...)):
    _check_auth(password)
    state["equipes"] = [e for e in state["equipes"] if e["id"] != id]
    _salvar(state)
    await broadcast("equipes", state)
    return {"ok": True}


@app.post("/admin/pontos")
async def pontuar(
    password: str = Form(...),
    id: int = Form(...),
    delta: int = Form(0),
    set_to: int = Form(-1),
):
    """delta = soma; set_to >= 0 sobrescreve."""
    _check_auth(password)
    eq = next((e for e in state["equipes"] if e["id"] == id), None)
    if not eq:
        raise HTTPException(404, "Equipe nao encontrada")
    if set_to >= 0:
        eq["pontos"] = set_to
    else:
        eq["pontos"] = max(0, eq["pontos"] + delta)
    _salvar(state)
    await broadcast("pontos", {"id": id, "pontos": eq["pontos"]})
    return {"ok": True, "pontos": eq["pontos"]}


@app.post("/admin/reset")
async def reset(password: str = Form(...)):
    _check_auth(password)
    for e in state["equipes"]:
        e["pontos"] = 0
    _salvar(state)
    await broadcast("equipes", state)
    return {"ok": True}


@app.get("/stream")
async def stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    subscribers.append(queue)
    ip = request.client.host if request.client else "?"
    print(f"[SSE] conectado: {ip} (total: {len(subscribers)})")

    async def gen():
        yield {"event": "snapshot", "data": json.dumps(state)}
        try:
            while True:
                msg = await queue.get()
                yield {"event": msg["event"], "data": json.dumps(msg["data"])}
        except asyncio.CancelledError:
            pass
        finally:
            try:
                subscribers.remove(queue)
            except ValueError:
                pass
            print(f"[SSE] desconectado: {ip} (total: {len(subscribers)})")

    return EventSourceResponse(
        gen(),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        ping=15,
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    print(f"Placar Innovathon 2026 - http://0.0.0.0:{port}")
    print(f"Admin password: {ADMIN_PASSWORD} (env PLACAR_ADMIN_PASSWORD)")
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
