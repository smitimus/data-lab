FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends     libpq-dev gcc git     && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core==1.11.12 dbt-postgres==1.10.2

WORKDIR /usr/app
