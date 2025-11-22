# Kafka → Databricks Streaming Pipeline

This project demonstrates a full real-time streaming architecture using:

- Local Kafka (Docker)
- Python API Producer
- Databricks Structured Streaming
- Delta Lake Bronze → Silver → Gold
- DLT Pipeline
- Databricks SQL dashboards

## Architecture

API Producer → Kafka → Autoloader → Delta Bronze → DLT (Silver, Gold)

## How to Run

### 1. Start Kafka

```bash
cd docker
docker compose up -d