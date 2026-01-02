#!/usr/bin/env bash
set -e

# кеші (не обов’язково, але корисно)
export HF_HOME=/cache/hf
export TORCH_HOME=/cache/torch
mkdir -p /cache /models /outputs

cd /ComfyUI

python3 /telemetry_agent.py &
TELEMETRY_PID=$!
trap "kill ${TELEMETRY_PID}" EXIT

COMFY_LISTEN_ADDR=${COMFY_LISTEN_ADDR:-0.0.0.0}
COMFY_PORT=${COMFY_PORT:-3000}

exec python3 main.py \
  --listen "${COMFY_LISTEN_ADDR}" \
  --port "${COMFY_PORT}" \
  --disable-auto-launch \
  --enable-cors-header \
  --highvram
