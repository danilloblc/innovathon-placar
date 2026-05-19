FROM python:3.12-slim

WORKDIR /app

# Deps de sistema minimas
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates ./templates
COPY static ./static

# Volume para persistir data/equipes.json fora do container
VOLUME ["/app/data"]

ENV PORT=8000
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:${PORT}/version || exit 1

CMD ["python", "app.py"]
