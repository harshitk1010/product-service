#!/usr/bin/env bash
# ── Local development setup script ───────────────────────────────────────────
set -euo pipefail

echo "==> Setting up E-Commerce Analytics Platform (local dev)"

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Docker is required. Install from https://docs.docker.com/get-docker/"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Python 3.11+ required"; exit 1; }

# Create .env if not exists
if [ ! -f .env ]; then
  cp .env.example .env
  echo "==> Created .env from .env.example — fill in your credentials"
fi

# Install Python dependencies
echo "==> Installing Python dependencies..."
pip install -r services/event_producer/requirements.txt

# Start Docker stack
echo "==> Starting Docker services (Kafka, PostgreSQL, Spark, Airflow)..."
cd infrastructure/docker
docker-compose up -d

# Wait for Kafka
echo "==> Waiting for Kafka to be ready..."
until docker-compose exec kafka-1 kafka-broker-api-versions \
  --bootstrap-server localhost:9092 >/dev/null 2>&1; do
  sleep 2
  echo "  ...waiting for Kafka..."
done

# Create topics
echo "==> Creating Kafka topics..."
cd ../../data_platform/kafka/topics
python topics_config.py

echo ""
echo "==> Setup complete! Services:"
echo "  Kafka UI:   http://localhost:8080"
echo "  Grafana:    http://localhost:3000  (admin/admin)"
echo "  Airflow:    http://localhost:8083  (admin/admin)"
echo "  Spark UI:   http://localhost:8082"
echo "  Prometheus: http://localhost:9090"
echo ""
echo "==> Start the event producer:"
echo "  cd services/event_producer && uvicorn main:app --reload"
echo ""
echo "==> Simulate events:"
echo "  curl -X POST 'http://localhost:8000/simulate/sessions?n_sessions=100'"
