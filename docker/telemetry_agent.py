#!/usr/bin/env python3
"""
Runpod Telemetry Agent (Python)
- Exposes /metrics (JSON), /stream (SSE), /ws (WebSocket)
- Optionally pushes metrics to remote HTTP/WS endpoints via env vars
- Designed to run inside a Runpod pod, but works on будь-якій Linux VM

ENV:
  PORT=8000
  TELEMETRY_INTERVAL=2.0     # seconds
  AUTH_TOKEN=...             # optional, if set -> require Bearer token
  PUSH_HTTP_URL=...          # optional, POST JSON here each interval
  PUSH_WS_URL=...            # optional, ws:// or wss:// to send JSON frames
  PUSH_TOKEN=...             # optional auth token for push (Bearer)
"""

import asyncio
import json
import os
import platform
import socket
import time
from typing import Any, Dict, List, Optional

import psutil
from fastapi import FastAPI, Depends, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.websockets import WebSocketState
import uvicorn
import subprocess

# --- Auth dependency (optional) ------------------------------------------------
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

async def require_auth(authorization: Optional[str] = Header(None)):
    if not AUTH_TOKEN:
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

# --- GPU helpers ---------------------------------------------------------------
def _nvml_available() -> bool:
    try:
        import pynvml  # noqa
        return True
    except Exception:
        return False

def _collect_gpu_nvml() -> List[Dict[str, Any]]:
    import pynvml
    gpus = []
    try:
        pynvml.nvmlInit()
        count = pynvml.nvmlDeviceGetCount()
        for i in range(count):
            h = pynvml.nvmlDeviceGetHandleByIndex(i)
            name = pynvml.nvmlDeviceGetName(h).decode()
            uuid = pynvml.nvmlDeviceGetUUID(h).decode()
            mem = pynvml.nvmlDeviceGetMemoryInfo(h)
            temp = None
            util_gpu = None
            power = None
            fan = None
            try:
                temp = pynvml.nvmlDeviceGetTemperature(h, pynvml.NVML_TEMPERATURE_GPU)
            except Exception:
                pass
            try:
                util = pynvml.nvmlDeviceGetUtilizationRates(h)
                util_gpu = util.gpu
            except Exception:
                pass
            try:
                power = pynvml.nvmlDeviceGetPowerUsage(h) / 1000.0  # W
            except Exception:
                pass
            try:
                fan = pynvml.nvmlDeviceGetFanSpeed(h)
            except Exception:
                pass

            gpus.append({
                "index": i,
                "name": name,
                "uuid": uuid,
                "memory_total_mb": round(mem.total / (1024**2), 2),
                "memory_used_mb": round(mem.used / (1024**2), 2),
                "utilization_gpu_percent": util_gpu,
                "temperature_c": temp,
                "power_w": power,
                "fan_percent": fan,
            })
    finally:
        try:
            pynvml.nvmlShutdown()
        except Exception:
            pass
    return gpus

def _collect_gpu_nvidia_smi() -> List[Dict[str, Any]]:
    # Fallback if NVML python package is unavailable
    try:
        cmd = [
            "nvidia-smi",
            "--query-gpu=name,uuid,temperature.gpu,utilization.gpu,memory.total,memory.used,power.draw",
            "--format=csv,noheader,nounits",
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True).strip()
        gpus = []
        if not out:
            return gpus
        for idx, line in enumerate(out.splitlines()):
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 7:
                continue
            name, uuid, temp, util, mem_total, mem_used, power = parts[:7]
            gpus.append({
                "index": idx,
                "name": name,
                "uuid": uuid,
                "temperature_c": _to_int(temp),
                "utilization_gpu_percent": _to_int(util),
                "memory_total_mb": _to_float(mem_total),
                "memory_used_mb": _to_float(mem_used),
                "power_w": _to_float(power),
            })
        return gpus
    except Exception:
        return []

def _to_int(x):
    try:
        return int(float(x))
    except Exception:
        return None

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

# --- Metrics collection --------------------------------------------------------
_prev_net = psutil.net_io_counters()

def collect_metrics() -> Dict[str, Any]:
    global _prev_net
    ts = time.time()
    uname = platform.uname()
    cpu_percent = psutil.cpu_percent(interval=None)
    load = None
    try:
        load = os.getloadavg()
    except Exception:
        pass
    vm = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    # rates
    interval = max(0.001, ts - getattr(collect_metrics, "_last_ts", ts))
    tx_rate = (net.bytes_sent - _prev_net.bytes_sent) / interval
    rx_rate = (net.bytes_recv - _prev_net.bytes_recv) / interval
    _prev_net = net
    collect_metrics._last_ts = ts  # type: ignore

    # GPUs
    if _nvml_available():
        gpus = _collect_gpu_nvml()
    else:
        gpus = _collect_gpu_nvidia_smi()

    data = {
        "ts": ts,
        "host": {
            "hostname": socket.gethostname(),
            "os": f"{uname.system} {uname.release}",
            "kernel": uname.version,
            "python": platform.python_version(),
        },
        "runpod": {
            "pod_id": os.getenv("RUNPOD_POD_ID"),
            "region": os.getenv("RUNPOD_REGION"),
            "endpoint_id": os.getenv("RUNPOD_ENDPOINT_ID"),
            "vcpu_count": os.getenv("RUNPOD_VCPU_COUNT"),
            "gpu_count": len(gpus) if gpus else 0,
        },
        "cpu": {
            "percent": cpu_percent,
            "cores_logical": psutil.cpu_count(logical=True),
            "cores_physical": psutil.cpu_count(logical=False),
            "load_1_5_15": load,
        },
        "memory": {
            "total_mb": round(vm.total / (1024**2), 2),
            "used_mb": round(vm.used / (1024**2), 2),
            "free_mb": round(vm.available / (1024**2), 2),
            "percent": vm.percent,
        },
        "swap": {
            "total_mb": round(swap.total / (1024**2), 2),
            "used_mb": round(swap.used / (1024**2), 2),
            "percent": swap.percent,
        },
        "disk_root": {
            "total_gb": round(disk.total / (1024**3), 2),
            "used_gb": round(disk.used / (1024**3), 2),
            "free_gb": round(disk.free / (1024**3), 2),
            "percent": disk.percent,
        },
        "network": {
            "bytes_sent": net.bytes_sent,
            "bytes_recv": net.bytes_recv,
            "tx_rate_bps": tx_rate,
            "rx_rate_bps": rx_rate,
        },
        "gpus": gpus,
        "process": {
            "uptime_sec": _uptime(),
        }
    }
    return data

def _uptime():
    try:
        return int(time.time() - psutil.boot_time())
    except Exception:
        return None

# --- FastAPI app ---------------------------------------------------------------
app = FastAPI(title="Runpod Telemetry Agent")
INTERVAL = float(os.getenv("TELEMETRY_INTERVAL", "2.0"))

@app.get("/metrics")
async def metrics(_: Any = Depends(require_auth)):
    return JSONResponse(collect_metrics())

@app.get("/stream")
async def stream(_: Any = Depends(require_auth)):
    async def event_gen():
        while True:
            payload = json.dumps(collect_metrics(), ensure_ascii=False)
            yield f"data: {payload}\n\n"
            await asyncio.sleep(INTERVAL)
    return StreamingResponse(event_gen(), media_type="text/event-stream")

# simple per-connection streamer; keeps code compact
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    # if AUTH_TOKEN set, require ?token=...
    if AUTH_TOKEN:
        token = websocket.query_params.get("token")
        if token != AUTH_TOKEN:
            await websocket.close(code=4403)
            return
    await websocket.accept()
    try:
        while True:
            if websocket.application_state != WebSocketState.CONNECTED:
                break
            await websocket.send_text(json.dumps(collect_metrics(), ensure_ascii=False))
            # optional: receive pings/interval updates from client
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=INTERVAL)
                # you could parse commands here (e.g., change interval)
            except asyncio.TimeoutError:
                pass
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await websocket.close()
        except Exception:
            pass

# --- Optional pushers ----------------------------------------------------------
async def push_http(session, url: str, token: Optional[str]):
    import aiohttp
    while True:
        data = collect_metrics()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            async with session.post(url, json=data, headers=headers, timeout=10) as resp:
                await resp.read()  # drain
        except Exception:
            pass
        await asyncio.sleep(INTERVAL)

async def push_ws(session, url: str, token: Optional[str]):
    import aiohttp
    params = {}
    headers = {}
    if token:
        # try Bearer header first; if server expects query, adapt as needed
        headers["Authorization"] = f"Bearer {token}"
    while True:
        try:
            async with session.ws_connect(url, headers=headers, params=params, timeout=10) as ws:
                while True:
                    await ws.send_json(collect_metrics())
                    await asyncio.sleep(INTERVAL)
        except Exception:
            await asyncio.sleep(5)  # backoff and retry

@app.on_event("startup")
async def startup_pushers():
    push_http_url = os.getenv("PUSH_HTTP_URL")
    push_ws_url = os.getenv("PUSH_WS_URL")
    push_token = os.getenv("PUSH_TOKEN")
    if push_http_url or push_ws_url:
        import aiohttp
        session = aiohttp.ClientSession()
        if push_http_url:
            asyncio.create_task(push_http(session, push_http_url, push_token))
        if push_ws_url:
            asyncio.create_task(push_ws(session, push_ws_url, push_token))

def main():
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("telemetry_agent:app", host="0.0.0.0", port=port, log_level="info")

if __name__ == "__main__":
    main()
