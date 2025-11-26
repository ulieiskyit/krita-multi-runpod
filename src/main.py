"""Entrypoint for the Comfy gateway service."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI

from src.gateway import GatewayConfig, create_app
from src.runpod_manager import RunpodClient, RunpodSessionManager

load_dotenv(override=False)

logger = logging.getLogger("comfy.main")
logging.getLogger("comfy").setLevel(logging.DEBUG)


def _env_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.lower() not in {"0", "false", "no", "off"}


def _optional_int(raw: Optional[str]) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Expected integer value, got {raw!r}") from exc


def _optional_float(raw: Optional[str]) -> Optional[float]:
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Expected float value, got {raw!r}") from exc


def _load_json_file(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:  # pragma: no cover - config error
        raise RuntimeError(f"RunPod config file not found: {path}") from exc
    except json.JSONDecodeError as exc:  # pragma: no cover - config error
        raise RuntimeError(f"RunPod config file {path} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):  # pragma: no cover - config error
        raise RuntimeError(f"RunPod config {path} must contain a JSON object")
    return data


def _load_runpod_config() -> dict:
    config_path = os.getenv("RUNPOD_CONFIG_PATH")
    if not config_path:
        return {}
    return _load_json_file(Path(config_path))


def _load_extra_payload(raw: Optional[str]) -> dict:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:  # pragma: no cover - configuration error
        raise RuntimeError("RUNPOD_EXTRA_PAYLOAD must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("RUNPOD_EXTRA_PAYLOAD must decode to a JSON object")
    return payload


def _first_set_value(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def build_session_manager(config: GatewayConfig, runpod_cfg: Optional[dict] = None) -> Optional[RunpodSessionManager]:
    runpod_cfg = runpod_cfg or _load_runpod_config()

    api_key = _first_set_value(os.getenv("RUNPOD_API_KEY"), runpod_cfg.get("api_key"))
    image_name = _first_set_value(runpod_cfg.get("image_name"), os.getenv("RUNPOD_IMAGE_NAME"))
    template_id = _first_set_value(runpod_cfg.get("template_id"), os.getenv("RUNPOD_TEMPLATE_ID"))
    if not api_key:
        logger.warning("RunPod support disabled — missing RUNPOD_API_KEY")
        return None
    if not template_id and not image_name:
        logger.warning("RunPod support disabled — provide template_id in config or image_name override")
        return None

    api_base = _first_set_value(runpod_cfg.get("api_base"), os.getenv("RUNPOD_API_BASE"), "https://api.runpod.io")
    proxy_port_raw = _first_set_value(runpod_cfg.get("proxy_port"), os.getenv("RUNPOD_PROXY_PORT"))
    proxy_port = int(proxy_port_raw) if proxy_port_raw is not None else 8188
    proxy_template = _first_set_value(
        runpod_cfg.get("proxy_template"),
        os.getenv("RUNPOD_PROXY_TEMPLATE"),
        f"https://{{pod_id}}-{proxy_port}.proxy.runpod.net",
    )

    startup_timeout = _first_set_value(runpod_cfg.get("startup_timeout_s"), _optional_float(os.getenv("RUNPOD_STARTUP_TIMEOUT_S")), 360.0)
    poll_interval = _first_set_value(runpod_cfg.get("startup_poll_s"), _optional_float(os.getenv("RUNPOD_STARTUP_POLL_S")), 5.0)
    idle_timeout = _first_set_value(runpod_cfg.get("idle_timeout_s"), _optional_float(os.getenv("RUNPOD_IDLE_TIMEOUT_S")), 900.0)
    failure_backoff = _first_set_value(runpod_cfg.get("failure_backoff_s"), _optional_float(os.getenv("RUNPOD_FAILURE_BACKOFF_S")), 30.0)
    request_timeout = _first_set_value(runpod_cfg.get("request_timeout_s"), _optional_float(os.getenv("RUNPOD_REQUEST_TIMEOUT_S")), 30.0)

    cfg_extra = runpod_cfg.get("extra_payload")
    if cfg_extra is not None and not isinstance(cfg_extra, dict):  # pragma: no cover - config error
        raise RuntimeError("RunPod config extra_payload must be a JSON object")
    extra_payload = cfg_extra or _load_extra_payload(os.getenv("RUNPOD_EXTRA_PAYLOAD"))

    gpu_count = _first_set_value(runpod_cfg.get("gpu_count"), _optional_int(os.getenv("RUNPOD_GPU_COUNT")))
    volume_gb = _first_set_value(runpod_cfg.get("volume_gb"), _optional_int(os.getenv("RUNPOD_VOLUME_GB")))
    max_sessions = _first_set_value(runpod_cfg.get("max_active_sessions"), _optional_int(os.getenv("RUNPOD_MAX_ACTIVE_SESSIONS")))

    telemetry_template = _first_set_value(runpod_cfg.get("telemetry_template"), os.getenv("RUNPOD_TELEMETRY_TEMPLATE"))
    telemetry_port_value = _first_set_value(runpod_cfg.get("telemetry_port"), os.getenv("RUNPOD_TELEMETRY_PORT"))
    telemetry_port = int(telemetry_port_value) if telemetry_port_value not in (None, "") else None
    telemetry_path = _first_set_value(runpod_cfg.get("telemetry_path"), os.getenv("RUNPOD_TELEMETRY_PATH"), "/metrics")
    telemetry_poll = _first_set_value(runpod_cfg.get("telemetry_poll_s"), _optional_float(os.getenv("RUNPOD_TELEMETRY_POLL_S")), 60.0)
    telemetry_idle = _first_set_value(runpod_cfg.get("telemetry_idle_s"), _optional_float(os.getenv("RUNPOD_TELEMETRY_IDLE_S")), 600.0)
    telemetry_threshold = _first_set_value(
        runpod_cfg.get("telemetry_gpu_threshold"),
        _optional_float(os.getenv("RUNPOD_TELEMETRY_GPU_THRESHOLD")),
        0.0,
    )
    pod_name_tag = _first_set_value(runpod_cfg.get("pod_name_tag"), os.getenv("RUNPOD_POD_NAME_TAG"), "comfy-gw")
    prompt_threshold_count = _first_set_value(
        runpod_cfg.get("prompt_threshold_count"),
        _optional_int(os.getenv("RUNPOD_PROMPT_THRESHOLD_COUNT")),
        5,
    )
    prompt_threshold_window = _first_set_value(
        runpod_cfg.get("prompt_threshold_window_s"),
        _optional_float(os.getenv("RUNPOD_PROMPT_THRESHOLD_WINDOW_S")),
        60.0,
    )

    client = RunpodClient(
        api_key=api_key,
        base_url=api_base,
        request_timeout=float(request_timeout),
        image_name=image_name,
    )

    manager = RunpodSessionManager(
        client=client,
        template_id=template_id,
        duty_url=config.duty_pod_url,
        proxy_template=proxy_template,
        proxy_port=proxy_port,
        health_path=config.health_path,
        health_timeout=config.health_timeout_s,
        startup_timeout=float(startup_timeout),
        poll_interval=float(poll_interval),
        idle_timeout=float(idle_timeout),
        failure_backoff=float(failure_backoff),
        extra_payload=extra_payload,
        cloud_type=_first_set_value(runpod_cfg.get("cloud_type"), os.getenv("RUNPOD_CLOUD_TYPE")),
        gpu_type_id=_first_set_value(runpod_cfg.get("gpu_type_id"), os.getenv("RUNPOD_GPU_TYPE_ID")),
        gpu_count=gpu_count,
        volume_gb=volume_gb,
        region=_first_set_value(runpod_cfg.get("region"), os.getenv("RUNPOD_REGION")),
        image_name=image_name,
        max_active_sessions=max_sessions,
        telemetry_template=telemetry_template,
        telemetry_port=telemetry_port,
        telemetry_path=telemetry_path,
        telemetry_poll_interval=float(telemetry_poll),
        telemetry_idle_timeout=float(telemetry_idle),
        telemetry_gpu_threshold=float(telemetry_threshold),
        pod_name_tag=pod_name_tag if isinstance(pod_name_tag, str) else "comfy-gw",
        prompt_threshold_count=int(prompt_threshold_count) if prompt_threshold_count is not None else 5,
        prompt_threshold_window=float(prompt_threshold_window),
    )

    logger.info("RunPod support enabled — template %s", template_id)
    return manager


def build_gateway_app() -> FastAPI:
    duty_pod_url = os.getenv("DUTY_POD_URL") or os.getenv("MODAL_WEB_URL")
    if not duty_pod_url:
        raise RuntimeError("Set DUTY_POD_URL (or legacy MODAL_WEB_URL) in the environment")

    runpod_cfg = _load_runpod_config()

    health_path = _first_set_value(os.getenv("HEALTH_PATH"), runpod_cfg.get("health_path"), "/queue")
    health_timeout = _first_set_value(_optional_float(os.getenv("HEALTH_TIMEOUT_S")), runpod_cfg.get("health_timeout_s"), 10.0)

    config = GatewayConfig(
        duty_pod_url=duty_pod_url,
        health_path=health_path,
        health_timeout_s=float(health_timeout),
        user_id_header=os.getenv("USER_ID_HEADER", "X-Comfy-User"),
        user_id_query_key=os.getenv("USER_ID_QUERY_KEY", "user_id"),
        user_id_cookie=os.getenv("USER_ID_COOKIE") or None,
        require_user_id=_env_bool(os.getenv("REQUIRE_USER_ID"), default=True),
        use_client_id=_env_bool(os.getenv("USE_CLIENT_ID"), default=True),
        use_client_ip=_env_bool(os.getenv("USE_CLIENT_IP"), default=False),
    )

    session_manager = build_session_manager(config, runpod_cfg=runpod_cfg)
    return create_app(config, session_manager)


app = build_gateway_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
