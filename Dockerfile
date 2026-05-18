FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-market-tool.txt /app/requirements-market-tool.txt
RUN pip install --no-cache-dir -r /app/requirements-market-tool.txt

COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["python3", "run_confirmed_pipeline.py", "--help"]
