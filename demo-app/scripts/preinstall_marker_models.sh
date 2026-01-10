#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MODELS_DIR="${PROJECT_ROOT}/models/marker-cache"

mkdir -p "${MODELS_DIR}"
cd "${PROJECT_ROOT}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  COMPOSE_CMD=(docker compose)
fi

printf 'Preloading Marker models into %s\n' "${MODELS_DIR}"

"${COMPOSE_CMD[@]}" run --rm marker python -c "from marker.models import create_model_dict; print('Downloading Marker artifacts... this may take a few minutes.'); create_model_dict(); print('Marker artifacts downloaded.')"
