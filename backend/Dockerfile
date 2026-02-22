FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MOZ_HEADLESS=1

WORKDIR /app

# Firefox is used by Selenium in app_core.
RUN apt-get update && apt-get install -y --no-install-recommends \
    firefox-esr \
    ca-certificates \
    wget \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

COPY . /app

EXPOSE 8000

CMD ["sh", "-c", "python -m uvicorn backend.app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
