"""Placar do II Innovathon Contagil 2026.

Modelo de dados:
  - equipes:     lista de {id, nome, cor_idx, cor_hex}
  - provas:      lista de {id, nome, tipo, max_pontos, criterios?}
                 tipo in {basica, dinamica, principal}
                 criterios = lista de {nome, max} (so para tipo=principal)
  - lancamentos: lista de {id, equipe_id, prova_id, valor, ts, criterios_valores?}
                 UNICO por (equipe_id, prova_id) - re-lancar substitui
                 criterios_valores = {nome_criterio: valor} (para tipo=principal)
  - animacao:    {count_ms, reorder_ms, flash_ms}

Pontos totais de uma equipe = soma dos lancamentos.
Endpoints documentados no README.
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

BASE = Path(__file__).parent
DATA_FILE = BASE / "data" / "equipes.json"
DATA_FILE.parent.mkdir(exist_ok=True)

ADMIN_PASSWORD = os.environ.get("PLACAR_ADMIN_PASSWORD", "innovathon2026")
# Token compartilhado com o ESP32 (buzzer). Sobrescreva via env no Coolify.
BUZZER_TOKEN = os.environ.get("BUZZER_TOKEN", "buzzer-innovathon-2026")
APP_VERSION = str(int(time.time()))

DEFAULT_ANIMACAO = {"count_ms": 500, "reorder_ms": 650, "flash_ms": 1200}

# Estado do buzzer (em memoria, efemero - reseta a cada rodada).
#   vencedor: 0 = ninguem, 1 = jogador 1, 2 = jogador 2
#   tempo_us: timestamp em microssegundos (do ESP32) de quando apertou
#   armado:   True = aceita buzz; False = ja tem vencedor (travado)
#   seq:      incrementa a cada mudanca (ESP32 detecta reset via mudanca de seq)
buzzer = {"vencedor": 0, "tempo_us": 0, "armado": True, "seq": 0, "ts": None}

# Estado de presenca do ESP32 (via WebSocket).
#   online: conexao WS viva
#   rssi:   forca do sinal WiFi (dBm) reportada nos pings
esp32 = {"online": False, "rssi": None, "ts": None}

# Referencia da conexao WebSocket ativa do ESP32 (so 1 dispositivo).
active_esp32_ws: "WebSocket | None" = None


def _provas_padrao() -> list[dict]:
    """Provas conforme edital INNOVATHON 2026."""
    return [
        {"id": 1, "nome": "Manipulação de Dados",      "tipo": "basica",    "max_pontos": 20, "criterios": None},
        {"id": 2, "nome": "Integração de Dados",       "tipo": "basica",    "max_pontos": 20, "criterios": None},
        {"id": 3, "nome": "Lógica e Documentação",     "tipo": "basica",    "max_pontos": 20, "criterios": None},
        {"id": 4, "nome": "Dinâmica Competitiva",      "tipo": "dinamica",  "max_pontos": 20, "criterios": None},
        {"id": 5, "nome": "Desafio Estratégico Principal", "tipo": "principal", "max_pontos": 60, "criterios": [
            {"nome": "Inovação e Relevância",       "max": 10},
            {"nome": "Aplicabilidade Real",         "max": 10},
            {"nome": "Protótipo Funcional",         "max": 25},
            {"nome": "Métricas Antes vs Depois",    "max": 10},
            {"nome": "Clareza da Apresentação",     "max": 5},
        ]},
    ]


def _carregar() -> dict:
    if DATA_FILE.exists():
        try:
            d = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            d = {}
    else:
        d = {}

    d.setdefault("equipes", [])
    d.setdefault("provas", _provas_padrao() if not d.get("provas") else d["provas"])
    d.setdefault("lancamentos", [])
    d.setdefault("animacao", DEFAULT_ANIMACAO.copy())
    for k, v in DEFAULT_ANIMACAO.items():
        d["animacao"].setdefault(k, v)
    # Migracao: equipes antigas tinham campo "pontos" - remove (calculado dinamicamente)
    for e in d["equipes"]:
        e.pop("pontos", None)
        e.setdefault("cor_idx", 1)
        e.setdefault("cor_hex", None)
    return d


def _salvar(estado: dict) -> None:
    DATA_FILE.write_text(json.dumps(estado, ensure_ascii=False, indent=2), encoding="utf-8")


state = _carregar()
subscribers: list[asyncio.Queue] = []


def _pontos_equipe(equipe_id: int) -> int:
    """Soma de todos os lancamentos de uma equipe."""
    return sum(l["valor"] for l in state["lancamentos"] if l["equipe_id"] == equipe_id)


def _state_publico() -> dict:
    """Estado com pontos pre-calculados nas equipes (para o cliente)."""
    return {
        "equipes": [
            {**e, "pontos": _pontos_equipe(e["id"])}
            for e in state["equipes"]
        ],
        "provas": state["provas"],
        "lancamentos": state["lancamentos"],
        "animacao": state["animacao"],
        "buzzer": buzzer,
        "esp32": esp32,
    }


async def broadcast(event: str, data: Any) -> None:
    payload = {"event": event, "data": data}
    for q in list(subscribers):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                subscribers.remove(q)
            except ValueError:
                pass


def _next_id(items: list[dict]) -> int:
    return max((x["id"] for x in items), default=0) + 1


def _next_cor() -> int:
    usadas = {e["cor_idx"] for e in state["equipes"] if not e.get("cor_hex")}
    for i in range(1, 6):  # palette principal: 1-5
        if i not in usadas:
            return i
    return (len(state["equipes"]) % 5) + 1


def _check_auth(password: str) -> None:
    if password != ADMIN_PASSWORD:
        raise HTTPException(401, "Senha invalida")


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


@app.get("/buzzer", response_class=HTMLResponse)
async def buzzer_screen(request: Request):
    """Tela dedicada do buzzer (separada do placar principal)."""
    return templates.TemplateResponse(
        "buzzer.html",
        {"request": request, "app_version": APP_VERSION},
        headers=NO_CACHE,
    )


@app.get("/version")
async def version():
    return {"version": APP_VERSION}


@app.get("/state")
async def get_state():
    return _state_publico()


# ====== EQUIPES ======

@app.post("/admin/equipe")
async def equipe_criar_renomear(
    password: str = Form(...),
    id: int = Form(0),
    nome: str = Form(...),
    cor_idx: int = Form(0),
    cor_hex: str = Form(""),
):
    """Cria (id=0) ou atualiza nome/cor da equipe. cor_idx 1-5. cor_hex sobrescreve cor_idx."""
    _check_auth(password)
    nome = nome.strip()
    if not nome:
        raise HTTPException(400, "Nome obrigatorio")
    cor_idx = cor_idx if 1 <= cor_idx <= 5 else 0
    hex_val = cor_hex.strip() or None
    if hex_val and not (hex_val.startswith("#") and len(hex_val) == 7):
        raise HTTPException(400, "cor_hex invalido (use #RRGGBB)")

    if id == 0:
        nova = {
            "id": _next_id(state["equipes"]),
            "nome": nome,
            "cor_idx": cor_idx or _next_cor(),
            "cor_hex": hex_val,
        }
        state["equipes"].append(nova)
    else:
        eq = next((e for e in state["equipes"] if e["id"] == id), None)
        if not eq:
            raise HTTPException(404, "Equipe nao encontrada")
        eq["nome"] = nome
        if cor_idx:
            eq["cor_idx"] = cor_idx
        # cor_hex eh sempre setado (None se vazio) para permitir limpar
        eq["cor_hex"] = hex_val
    _salvar(state)
    await broadcast("state", _state_publico())
    return {"ok": True}


@app.post("/admin/equipe/del")
async def equipe_remover(password: str = Form(...), id: int = Form(...)):
    _check_auth(password)
    state["equipes"] = [e for e in state["equipes"] if e["id"] != id]
    # Remove tambem lancamentos orfaos
    state["lancamentos"] = [l for l in state["lancamentos"] if l["equipe_id"] != id]
    _salvar(state)
    await broadcast("state", _state_publico())
    return {"ok": True}


# ====== PROVAS ======

@app.post("/admin/prova")
async def prova_criar_renomear(
    password: str = Form(...),
    id: int = Form(0),
    nome: str = Form(...),
    tipo: str = Form(...),
    max_pontos: int = Form(...),
    criterios_json: str = Form(""),
):
    """Cria (id=0) ou atualiza prova. criterios_json so para tipo=principal."""
    _check_auth(password)
    nome = nome.strip()
    if not nome:
        raise HTTPException(400, "Nome obrigatorio")
    if tipo not in {"basica", "dinamica", "principal"}:
        raise HTTPException(400, "tipo deve ser basica|dinamica|principal")
    if max_pontos <= 0:
        raise HTTPException(400, "max_pontos > 0")
    criterios = None
    if tipo == "principal":
        try:
            criterios = json.loads(criterios_json) if criterios_json else None
        except json.JSONDecodeError:
            raise HTTPException(400, "criterios_json invalido")
        if not isinstance(criterios, list) or not criterios:
            raise HTTPException(400, "Prova principal exige criterios (lista com nome e max)")
        soma = sum(c.get("max", 0) for c in criterios)
        if soma != max_pontos:
            raise HTTPException(400, f"Soma dos criterios ({soma}) deve igualar max_pontos ({max_pontos})")

    if id == 0:
        nova = {
            "id": _next_id(state["provas"]),
            "nome": nome,
            "tipo": tipo,
            "max_pontos": max_pontos,
            "criterios": criterios,
        }
        state["provas"].append(nova)
    else:
        pr = next((p for p in state["provas"] if p["id"] == id), None)
        if not pr:
            raise HTTPException(404, "Prova nao encontrada")
        pr["nome"] = nome
        pr["tipo"] = tipo
        pr["max_pontos"] = max_pontos
        pr["criterios"] = criterios
    _salvar(state)
    await broadcast("state", _state_publico())
    return {"ok": True}


@app.post("/admin/prova/del")
async def prova_remover(password: str = Form(...), id: int = Form(...)):
    _check_auth(password)
    state["provas"] = [p for p in state["provas"] if p["id"] != id]
    # Remove lancamentos orfaos
    state["lancamentos"] = [l for l in state["lancamentos"] if l["prova_id"] != id]
    _salvar(state)
    await broadcast("state", _state_publico())
    return {"ok": True}


# ====== LANCAMENTOS ======

@app.post("/admin/lancamento")
async def lancar(
    password: str = Form(...),
    equipe_id: int = Form(...),
    prova_id: int = Form(...),
    valor: int = Form(...),
    criterios_valores_json: str = Form(""),
):
    """Lanca/sobrescreve pontuacao de uma equipe em uma prova.

    valor: total da prova para a equipe (0 a max_pontos).
    criterios_valores_json: {nome_criterio: valor} (so para tipo=principal).
    """
    _check_auth(password)
    eq = next((e for e in state["equipes"] if e["id"] == equipe_id), None)
    if not eq:
        raise HTTPException(404, "Equipe nao encontrada")
    pr = next((p for p in state["provas"] if p["id"] == prova_id), None)
    if not pr:
        raise HTTPException(404, "Prova nao encontrada")
    if not 0 <= valor <= pr["max_pontos"]:
        raise HTTPException(400, f"valor deve estar entre 0 e {pr['max_pontos']}")

    crit_vals = None
    if pr["tipo"] == "principal":
        try:
            crit_vals = json.loads(criterios_valores_json) if criterios_valores_json else {}
        except json.JSONDecodeError:
            raise HTTPException(400, "criterios_valores_json invalido")
        # Valida cada criterio dentro do max
        nomes_validos = {c["nome"]: c["max"] for c in (pr["criterios"] or [])}
        for nome, val in crit_vals.items():
            if nome not in nomes_validos:
                raise HTTPException(400, f"Criterio desconhecido: {nome}")
            if not 0 <= val <= nomes_validos[nome]:
                raise HTTPException(400, f"Criterio '{nome}' fora do range 0..{nomes_validos[nome]}")
        soma = sum(crit_vals.values())
        if soma != valor:
            raise HTTPException(400, f"Soma dos criterios ({soma}) != valor ({valor})")

    # Procura lancamento existente (unico por equipe+prova)
    existente = next((l for l in state["lancamentos"]
                      if l["equipe_id"] == equipe_id and l["prova_id"] == prova_id), None)
    ts = datetime.now(timezone.utc).isoformat()
    if existente:
        existente["valor"] = valor
        existente["ts"] = ts
        existente["criterios_valores"] = crit_vals
    else:
        state["lancamentos"].append({
            "id": _next_id(state["lancamentos"]),
            "equipe_id": equipe_id,
            "prova_id": prova_id,
            "valor": valor,
            "ts": ts,
            "criterios_valores": crit_vals,
        })
    _salvar(state)
    await broadcast("state", _state_publico())
    return {"ok": True, "pontos_equipe": _pontos_equipe(equipe_id)}


@app.post("/admin/lancamento/del")
async def lancamento_remover(
    password: str = Form(...),
    equipe_id: int = Form(...),
    prova_id: int = Form(...),
):
    _check_auth(password)
    state["lancamentos"] = [
        l for l in state["lancamentos"]
        if not (l["equipe_id"] == equipe_id and l["prova_id"] == prova_id)
    ]
    _salvar(state)
    await broadcast("state", _state_publico())
    return {"ok": True}


@app.post("/admin/reset")
async def reset(password: str = Form(...)):
    """Zera todos os lancamentos (mantem equipes e provas)."""
    _check_auth(password)
    state["lancamentos"] = []
    _salvar(state)
    await broadcast("state", _state_publico())
    return {"ok": True}


# ====== BUZZER (integracao ESP32) ======

@app.get("/buzzer/state")
async def buzzer_state():
    """Estado leve para o ESP32 fazer polling (detectar reset via 'seq')."""
    return buzzer


@app.post("/buzzer/buzz")
async def buzzer_buzz(
    token: str = Form(...),
    player: int = Form(...),
    tempo_us: int = Form(0),
):
    """Recebe o buzz do ESP32. Quem chega primeiro trava (armado=False)."""
    if token != BUZZER_TOKEN:
        raise HTTPException(401, "Token invalido")
    if player not in (1, 2):
        raise HTTPException(400, "player deve ser 1 ou 2")
    if not buzzer["armado"]:
        # Ja tem vencedor - ignora (o ESP32 ja decidiu localmente de qualquer forma)
        return {"ok": False, "reason": "ja travado", "vencedor": buzzer["vencedor"]}
    buzzer["vencedor"] = player
    buzzer["tempo_us"] = tempo_us
    buzzer["armado"] = False
    buzzer["seq"] += 1
    buzzer["ts"] = datetime.now(timezone.utc).isoformat()
    await broadcast("buzzer", buzzer)
    return {"ok": True, "vencedor": player}


async def _do_reset():
    buzzer["vencedor"] = 0
    buzzer["tempo_us"] = 0
    buzzer["armado"] = True
    buzzer["seq"] += 1
    buzzer["ts"] = datetime.now(timezone.utc).isoformat()
    await broadcast("buzzer", buzzer)


@app.post("/buzzer/reset")
async def buzzer_reset(token: str = Form(""), password: str = Form("")):
    """Re-arma o buzzer. Aceita token (do ESP32) OU senha admin (da tela).
    Tambem empurra o comando de reset pro ESP32 via WebSocket (re-arma na hora)."""
    if token != BUZZER_TOKEN and password != ADMIN_PASSWORD:
        raise HTTPException(401, "Auth invalida (token ou senha)")
    await _do_reset()
    # empurra pro ESP32 re-armar localmente
    if active_esp32_ws is not None:
        try:
            await active_esp32_ws.send_json({"type": "reset"})
        except Exception:
            pass
    return {"ok": True}


@app.websocket("/buzzer/ws")
async def buzzer_ws(websocket: WebSocket):
    """Conexao persistente com o ESP32.

    ESP32 -> servidor:
      {"type":"hello","token":"...","rssi":-55}
      {"type":"buzz","player":1,"tempo_us":12345}
      {"type":"reset"}
      {"type":"ping","rssi":-55}
    servidor -> ESP32:
      {"type":"welcome","armado":true}
      {"type":"reset"}
    """
    global active_esp32_ws
    await websocket.accept()
    authed = False
    try:
        while True:
            # timeout: se o ESP32 nao mandar nada (nem ping) por 12s, assume morto
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=12)
            except asyncio.TimeoutError:
                break
            except Exception:
                break

            tipo = data.get("type")

            if tipo == "hello":
                if data.get("token") != BUZZER_TOKEN:
                    await websocket.close(code=4001)
                    return
                authed = True
                active_esp32_ws = websocket
                esp32["online"] = True
                esp32["rssi"] = data.get("rssi")
                esp32["ts"] = datetime.now(timezone.utc).isoformat()
                await broadcast("esp32", esp32)
                await websocket.send_json({"type": "welcome", "armado": buzzer["armado"]})
                print(f"[WS] ESP32 conectado (rssi={esp32['rssi']})")
                continue

            if not authed:
                continue

            if tipo == "buzz":
                player = data.get("player")
                if buzzer["armado"] and player in (1, 2):
                    buzzer["vencedor"] = player
                    buzzer["tempo_us"] = data.get("tempo_us", 0)
                    buzzer["armado"] = False
                    buzzer["seq"] += 1
                    buzzer["ts"] = datetime.now(timezone.utc).isoformat()
                    await broadcast("buzzer", buzzer)

            elif tipo == "reset":
                await _do_reset()

            elif tipo == "ping":
                esp32["online"] = True
                esp32["rssi"] = data.get("rssi")
                esp32["ts"] = datetime.now(timezone.utc).isoformat()
                await broadcast("esp32", esp32)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[WS] erro: {e}")
    finally:
        if active_esp32_ws is websocket:
            active_esp32_ws = None
            esp32["online"] = False
            esp32["ts"] = datetime.now(timezone.utc).isoformat()
            await broadcast("esp32", esp32)
            print("[WS] ESP32 desconectado")


# ====== ANIMACAO ======

@app.post("/admin/animacao")
async def set_animacao(
    password: str = Form(...),
    count_ms: int = Form(...),
    reorder_ms: int = Form(...),
    flash_ms: int = Form(...),
):
    _check_auth(password)
    state["animacao"] = {
        "count_ms": max(50, min(5000, count_ms)),
        "reorder_ms": max(100, min(5000, reorder_ms)),
        "flash_ms": max(100, min(5000, flash_ms)),
    }
    _salvar(state)
    await broadcast("animacao", state["animacao"])
    return {"ok": True, "animacao": state["animacao"]}


# ====== SSE ======

@app.get("/stream")
async def stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    subscribers.append(queue)
    ip = request.client.host if request.client else "?"
    print(f"[SSE] conectado: {ip} (total: {len(subscribers)})")

    async def gen():
        yield {"event": "snapshot", "data": json.dumps(_state_publico())}
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
