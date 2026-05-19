# Innovathon 2026 — Placar

Placar ao vivo do **II Innovathon Contágil 2026**. Painel admin para registrar pontos; ranking público atualiza em tempo real via Server-Sent Events.

- **Stack**: FastAPI + SSE + HTML/CSS/JS (vanilla).
- **Persistência**: arquivo JSON em `data/equipes.json` (sobrevive a restart do container).
- **Tempo real**: SSE com auto-reconnect e auto-reload de versão.

## Rodar local

```bash
pip install -r requirements.txt
python app.py
# http://localhost:8000/         placar publico
# http://localhost:8000/admin    painel
```

Senha padrão: `innovathon2026`. Sobrescreva com `PLACAR_ADMIN_PASSWORD=...`.

## Docker

```bash
docker compose up -d --build
```

Variáveis úteis em `.env`:

```
PLACAR_ADMIN_PASSWORD=sua_senha_aqui
```

## Deploy em VPS

Pré-requisitos: Docker + Docker Compose instalados.

```bash
# na VPS
git clone https://github.com/danilloblc/innovathon-placar.git
cd innovathon-placar
echo 'PLACAR_ADMIN_PASSWORD=trocar_essa_senha' > .env
docker compose up -d --build
```

A porta 8000 fica exposta. Coloque um reverse proxy (Caddy/Nginx) na frente para HTTPS.

### Caddyfile mínimo (exemplo)

```
placar.seu-dominio.com {
    reverse_proxy localhost:8000
}
```

## Endpoints

| Rota | Método | Descrição |
|---|---|---|
| `/` | GET | Placar público |
| `/admin` | GET | Painel admin |
| `/version` | GET | Versão do app (para auto-reload do front) |
| `/state` | GET | Estado atual em JSON |
| `/stream` | GET | SSE com eventos (`snapshot`, `equipes`, `pontos`) |
| `/admin/equipe` | POST | Criar/renomear equipe (`password`, `id=0` para criar, `nome`) |
| `/admin/equipe/del` | POST | Remover equipe (`password`, `id`) |
| `/admin/pontos` | POST | Atualizar pontos (`password`, `id`, `delta` ou `set_to`) |
| `/admin/reset` | POST | Zerar todas as pontuações (`password`) |

## Estrutura

```
.
├── app.py                  # FastAPI + SSE + persistência
├── templates/
│   ├── public.html         # placar público
│   └── admin.html          # painel admin
├── static/
│   └── style.css
├── data/
│   └── equipes.json        # estado persistido (volume Docker)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```
