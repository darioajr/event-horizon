#!/bin/bash
set -e

# Variáveis com defaults
BROKER_ID=${BROKER_ID:-0}
BROKER_HOST=${BROKER_HOST:-0.0.0.0}
BROKER_PORT=${BROKER_PORT:-9092}
LOG_LEVEL=${LOG_LEVEL:-info}
DATA_DIR=${EVENT_HORIZON_DATA:-/var/lib/eventhorizon}
CONFIG_FILE=${EVENT_HORIZON_CONFIG}/config.json

# Criar diretório de dados se não existir
mkdir -p "${DATA_DIR}"

echo "Starting Event Horizon..."
echo "  Broker ID: ${BROKER_ID}"
echo "  Host: ${BROKER_HOST}"
echo "  Port: ${BROKER_PORT}"
echo "  Data Dir: ${DATA_DIR}"
echo "  Log Level: ${LOG_LEVEL}"

exec ${EVENT_HORIZON_HOME}/bin/event_horizon \
    --config "${CONFIG_FILE}" \
    --port "${BROKER_PORT}" \
    --data "${DATA_DIR}" \
    --log-level "${LOG_LEVEL}" \
    "$@"
