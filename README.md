# Comfy RunPod Gateway

A FastAPI gateway that fronts one or more ComfyUI instances. Locally you can point it at a ComfyUI process running on your machine, and (optionally) let it spin up per-user GPU pods on RunPod when provided with API credentials.

## Prerequisites
- Python 3.10 or newer (3.11 recommended)
- A running ComfyUI instance for the fallback `duty` pod (default assumes `http://localhost:3000/`)
- Optional: RunPod account and API key if you want automatic pod provisioning

## Repository Layout
- `src/main.py` – loads environment/config and boots the FastAPI application
- `src/gateway.py` – HTTP/WebSocket proxy and request-routing logic
- `src/runpod_manager.py` – RunPod session lifecycle management
- `docker/` – image and startup bits used when building a RunPod ComfyUI template
- `download_models/` – helper utilities to fetch model assets defined in `manifest.json`

## 1. Prepare the fallback ComfyUI instance
The gateway always has a fallback upstream known as the "duty" pod. For local development you can run ComfyUI in another terminal (for example via `python main.py --listen 0.0.0.0 --port 3000` inside a ComfyUI checkout) and make sure it is reachable at the URL you configure in `.env`.

If you want to pull down the models referenced in `manifest.json`, run:

```
python download_models.py --base-dir models
```

Point your ComfyUI instance at the resulting `models/` folder (see `docker/extra_model_paths.yaml` for the layout that the provided Docker image expects).

## 2. Install Python dependencies
Clone the repository, then create a virtual environment and install the requirements from the project root:

```
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS / Linux
# source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

When the venv is active the gateway code will be available as the `src` package.

## 3. Configure environment variables
Copy `.env.example` to `.env` and adjust the values that matter for your setup. Every variable is documented inline, for example:

- `DUTY_POD_URL` – base URL of the always-on ComfyUI instance you want to forward to locally
- `USER_ID_HEADER` / `USER_ID_QUERY_KEY` – how clients identify themselves; set `REQUIRE_USER_ID=0` to allow anonymous access without spawning private pods
- `RUNPOD_API_KEY` – leave blank to disable RunPod provisioning, or supply your actual key
- `RUNPOD_CONFIG_PATH` – JSON file describing pod template, proxy pattern, GPU sizing, etc. (`runpod.config.json` is provided as a starting point)
- `RUNPOD_POD_NAME_TAG` – prefix tag that gets prepended to every pod name so the gateway only manages pods it created
- `RUNPOD_PROMPT_THRESHOLD_COUNT` / `RUNPOD_PROMPT_THRESHOLD_WINDOW_S` – how many prompts a user must submit within a rolling window before a dedicated RunPod pod is provisioned (defaults: 5 prompts in 60s)
- `RUNPOD_TELEMETRY_TEMPLATE` / `RUNPOD_TELEMETRY_PORT` – where the gateway should poll the pod telemetry agent (defaults assume RunPod proxies port 8000)
- `RUNPOD_TELEMETRY_IDLE_S` – how long the GPU can sit at 0% utilisation before the gateway shuts the pod down (default 600s)

Environment variables override values inside `runpod.config.json`, so you can tweak things like GPU type or idle timeout without editing the file.

## 4. Run the gateway locally
With the virtual environment activated and `.env` in place, start Uvicorn from the project root so Python finds the `src` package:

```
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

Visit `http://localhost:8000/` to confirm the gateway is up. HTTP requests (and WebSocket connections to `/ws`) will be proxied to the fallback ComfyUI until a dedicated RunPod session is ready for the requesting user.

If you disable RunPod support (no API key), the gateway simply fronts your local ComfyUI.

## Optional: Use RunPod sessions locally
To exercise the RunPod workflow from your workstation:

1. Ensure `RUNPOD_API_KEY` (and optionally `RUNPOD_TEMPLATE_ID` / `RUNPOD_IMAGE_NAME`) are set in `.env` or your shell.
2. Update `runpod.config.json` with values that match the template you built for ComfyUI. The `proxy_template` must correspond to the public URL RunPod assigns to your pods. By default the gateway also expects a telemetry agent to be reachable at `https://{pod_id}-8000.proxy.runpod.net/metrics` (configurable with the telemetry fields). Pods launched by the gateway are automatically prefixed with the tag from `RUNPOD_POD_NAME_TAG` so the watcher knows which pods it owns.
3. Start the gateway. Users will continue hitting the duty pod until they exceed the configured prompt threshold (default: 5 prompts within 60 seconds), at which point the gateway provisions a dedicated RunPod instance in the background. If you do not set an explicit user header, the gateway derives identity from the `client_id` inside ComfyUI prompt requests (preferred) or, as a last resort, the client IP.
4. Once the dedicated pod passes health checks, responses include `X-Comfy-Session-Ready: 1` and traffic is automatically routed to the private endpoint.
5. When GPU utilisation stays at 0% for longer than the configured telemetry idle window, the gateway automatically terminates the tagged pod to save credits. Any pod without the tag is ignored.

## Optional: Build the ComfyUI RunPod image
The `docker/` directory contains everything needed to build an image that RunPod can run (including the telemetry agent the watcher polls):

```
cd docker
docker build -t comfy-runpod-comfyui .
```

That image launches ComfyUI via `start.sh` and also runs `telemetry_agent.py` to expose hardware metrics on port 8000.

## Telemetry agent
Inside a RunPod pod the sidecar FastAPI app in `docker/telemetry_agent.py` exposes system metrics via HTTP (`/metrics`), Server-Sent Events (`/stream`), and WebSocket (`/ws`). You can run it locally via:

```
python docker/telemetry_agent.py
```

Set `AUTH_TOKEN` if you want to protect those endpoints while testing.

## Troubleshooting
- Gateway fails to start: run from the project root and use `uvicorn src.main:app …`; verify `.env` contains a valid `DUTY_POD_URL` and the file is being loaded (it uses `python-dotenv`).
- Requests return 400 about missing user ID: either send the header configured by `USER_ID_HEADER` or set `REQUIRE_USER_ID=0` in `.env` for local experiments.
- RunPod pods never become ready: check your template’s `proxy_template`, `health_path`, and GPU availability; logs appear in the gateway process (look for `RunPod session` messages).

Once both the gateway and ComfyUI are running locally, clients can talk to `http://localhost:8000/` and the gateway handles routing, health checks, and (if enabled) RunPod session orchestration.
