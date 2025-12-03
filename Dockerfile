FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    unixodbc-dev \
    gnupg2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src
COPY start.sh /app/start.sh
COPY stop.sh /app/stop.sh
RUN chmod +x /app/start.sh /app/stop.sh

ENTRYPOINT ["/bin/bash"]
