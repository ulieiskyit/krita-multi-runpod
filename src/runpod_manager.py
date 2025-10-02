"""RunPod session lifecycle utilities."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional

import httpx
import runpod

logger = logging.getLogger("comfy.runpod")

READY_STATUSES = {"RUNNING"}
FAILURE_STATUSES = {"CANCELLED", "DELETED", "FAILED", "STOPPED", "TERMINATED"}


@dataclass
class RunpodSession:
    user_id: str
    state: str = "pending"
    pod_id: Optional[str] = None
    base_url: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)
    ready_at: Optional[float] = None
    error: Optional[str] = None
    failure_count: int = 0
    last_state_change: float = field(default_factory=time.time)
    last_gpu_activity: float = field(default_factory=time.time)
    tagged: bool = False
    provision_task: Optional[asyncio.Task] = field(default=None, repr=False, compare=False)
    telemetry_task: Optional[asyncio.Task] = field(default=None, repr=False, compare=False)
    prompt_history: Deque[float] = field(default_factory=deque, repr=False, compare=False)


@dataclass
class UpstreamTarget:
    url: str
    session_ready: bool
    via_duty: bool
    user_id: Optional[str] = None
    session: Optional[RunpodSession] = None


def _slugify(value: str) -> str:
    keep = []
    for ch in value:
        if ch.isalnum() or ch in {"-", "_"}:
            keep.append(ch.lower())
        else:
            keep.append("-")
    slug = "".join(keep).strip("-")
    return slug or "user"


class RunpodClient:
    def __init__(self, api_key: str, base_url: str, request_timeout: float, *, image_name: Optional[str] = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._request_timeout = request_timeout
        self._image_name = image_name
        os.environ["RUNPOD_API_BASE_URL"] = self._base_url
        runpod.api_key = api_key

    async def create_pod(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await asyncio.to_thread(self._create_pod_sync, payload)

    def _create_pod_sync(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        args = payload.copy()
        if "image_name" not in args or args["image_name"] is None:
            args["image_name"] = self._image_name or args.get("template_id") or ""
        response = runpod.create_pod(**args)
        if not isinstance(response, dict):
            raise RuntimeError(f"Unexpected RunPod create_pod response type: {type(response)!r}")
        return response

    async def get_pod(self, pod_id: str) -> Dict[str, Any]:
        return await asyncio.to_thread(runpod.get_pod, pod_id)

    async def terminate_pod(self, pod_id: str) -> None:
        await asyncio.to_thread(runpod.terminate_pod, pod_id)

    async def close(self) -> None:
        # SDK is stateless; nothing to close
        return None

    async def safe_terminate(self, pod_id: Optional[str]) -> None:
        if not pod_id:
            return
        try:
            await self.terminate_pod(pod_id)
        except Exception as exc:  # pragma: no cover - best effort cleanup
            logger.warning("Failed to terminate pod %s cleanly: %s", pod_id, exc)


class RunpodSessionManager:
    def __init__(
        self,
        *,
        client: RunpodClient,
        template_id: str,
        duty_url: str,
        proxy_template: str,
        proxy_port: int,
        health_path: str,
        health_timeout: float,
        startup_timeout: float,
        poll_interval: float,
        idle_timeout: float,
        failure_backoff: float,
        extra_payload: Optional[Dict[str, Any]] = None,
        cloud_type: Optional[str] = None,
        gpu_type_id: Optional[str] = None,
        gpu_count: Optional[int] = None,
        volume_gb: Optional[int] = None,
        region: Optional[str] = None,
        image_name: Optional[str] = None,
        max_active_sessions: Optional[int] = None,
        telemetry_template: Optional[str] = None,
        telemetry_port: Optional[int] = 8000,
        telemetry_path: str = "/metrics",
        telemetry_poll_interval: float = 60.0,
        telemetry_idle_timeout: float = 600.0,
        telemetry_gpu_threshold: float = 0.0,
        pod_name_tag: str = "comfy-gw",
        prompt_threshold_count: int = 5,
        prompt_threshold_window: float = 60.0,
    ) -> None:
        self.client = client
        self.template_id = template_id
        self.duty_url = duty_url.rstrip("/")
        self.proxy_template = proxy_template
        self.proxy_port = proxy_port
        self.health_path = health_path
        self.health_timeout = health_timeout
        self.startup_timeout = startup_timeout
        self.poll_interval = poll_interval
        self.idle_timeout = idle_timeout
        self.failure_backoff = failure_backoff
        self.extra_payload = extra_payload or {}
        self.cloud_type = cloud_type
        self.gpu_type_id = gpu_type_id
        self.gpu_count = gpu_count
        self.volume_gb = volume_gb
        self.region = region
        self.image_name = image_name
        self.max_active_sessions = max_active_sessions

        self.telemetry_template = telemetry_template
        self.telemetry_port = telemetry_port
        self.telemetry_path = telemetry_path if telemetry_path.startswith("/") else f"/{telemetry_path}"
        self.telemetry_poll_interval = telemetry_poll_interval
        self.telemetry_idle_timeout = telemetry_idle_timeout
        self.telemetry_gpu_threshold = telemetry_gpu_threshold
        self.pod_name_tag = pod_name_tag
        self._tag_slug = _slugify(pod_name_tag) if pod_name_tag else ""
        if not self._tag_slug:
            self._tag_slug = "comfy-gw"

        self.prompt_threshold_count = max(0, int(prompt_threshold_count or 0))
        self.prompt_threshold_window = float(prompt_threshold_window or 0.0)

        self._sessions: Dict[str, RunpodSession] = {}
        self._lock = asyncio.Lock()
        self._health_client = httpx.AsyncClient(timeout=health_timeout)
        self._telemetry_client: Optional[httpx.AsyncClient] = None
        if self.telemetry_template:
            self._telemetry_client = httpx.AsyncClient(timeout=10.0)
        self._reaper_task: Optional[asyncio.Task] = None
        self._closing = False

    async def start(self) -> None:
        if self._reaper_task is None:
            self._reaper_task = asyncio.create_task(self._reaper(), name="runpod-session-reaper")

    async def shutdown(self) -> None:
        self._closing = True
        if self._reaper_task:
            self._reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reaper_task
        async with self._lock:
            sessions = list(self._sessions.keys())
        for user_id in sessions:
            await self._terminate_session(user_id, reason="shutdown")
        await self._health_client.aclose()
        if self._telemetry_client:
            await self._telemetry_client.aclose()
        await self.client.close()

    async def resolve(self, user_id: str) -> UpstreamTarget:
        now = time.time()
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                if self.max_active_sessions and len(self._sessions) >= self.max_active_sessions:
                    raise RuntimeError("RunPod session limit reached")
                session = RunpodSession(user_id=user_id, created_at=now, last_activity=now, last_state_change=now)
                self._sessions[user_id] = session
            else:
                session.last_activity = now
                if session.state == "failed" and self._can_retry(session, now):
                    self._reset_for_retry(session)
        if session.state == "ready":
            self._ensure_telemetry_task(session)
        via_duty = not (session.state == "ready" and session.base_url)
        url = session.base_url if session.base_url and not via_duty else self.duty_url
        return UpstreamTarget(url=url, session_ready=not via_duty, via_duty=via_duty, user_id=user_id, session=session)

    async def bump_activity(self, user_id: str) -> None:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session:
                session.last_activity = time.time()

    async def _terminate_session(self, user_id: str, reason: str) -> None:
        async with self._lock:
            session = self._sessions.pop(user_id, None)
        if not session:
            return
        logger.info("Terminating RunPod session for %s (%s)", user_id, reason)
        if session.provision_task and not session.provision_task.done():
            session.provision_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await session.provision_task
        if session.telemetry_task and not session.telemetry_task.done():
            session.telemetry_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await session.telemetry_task
        session.telemetry_task = None
        if session.tagged:
            await self.client.safe_terminate(session.pod_id)
        else:
            logger.info(
                "Skipping termination for %s (pod %s) due to missing tag", user_id, session.pod_id or "<unknown>"
            )
        session.pod_id = None
        session.base_url = None
        session.state = "terminated"

    def _ensure_provision_task(self, session: RunpodSession) -> None:
        if session.provision_task and not session.provision_task.done():
            return
        task = asyncio.create_task(self._provision_session(session), name=f"runpod-provision-{session.user_id}")
        session.provision_task = task

        def _done(t: asyncio.Task) -> None:
            with contextlib.suppress(asyncio.CancelledError):
                exc = t.exception()
                if exc:
                    logger.error("Provision task for %s failed: %s", session.user_id, exc)

        task.add_done_callback(_done)

    async def register_prompt(self, user_id: str) -> None:
        if self.prompt_threshold_count == 0:
            # threshold disabled -> provision immediately
            await self._trigger_provision(user_id)
            return
        now = time.time()
        session: Optional[RunpodSession] = None
        should_provision = False
        prompt_count = 0
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                session = RunpodSession(user_id=user_id, created_at=now, last_activity=now, last_state_change=now)
                self._sessions[user_id] = session
            if session.state == "ready":
                return
            if session.provision_task and not session.provision_task.done():
                return
            session.last_activity = now
            self._record_prompt(session, now)
            prompt_count = len(session.prompt_history)
            if self._should_start_provision(session):
                if session.state == "failed":
                    if self._can_retry(session, now):
                        self._reset_for_retry(session)
                        should_provision = True
                    else:
                        should_provision = False
                elif session.state in {"pending"}:
                    should_provision = True
        if should_provision and session is not None:
            logger.info(
                "Prompt threshold reached for %s (%d prompts)",
                user_id,
                prompt_count,
            )
            self._ensure_provision_task(session)

    async def _trigger_provision(self, user_id: str) -> None:
        now = time.time()
        session: Optional[RunpodSession] = None
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                session = RunpodSession(user_id=user_id, created_at=now, last_activity=now, last_state_change=now)
                self._sessions[user_id] = session
            session.last_activity = now
            if session.state == "ready":
                return
            if session.provision_task and not session.provision_task.done():
                return
            if session.state == "failed" and self._can_retry(session, now):
                self._reset_for_retry(session)
        if session:
            self._ensure_provision_task(session)

    def _ensure_telemetry_task(self, session: RunpodSession) -> None:
        if not self.telemetry_template or not self._telemetry_client:
            return
        if not session.pod_id:
            return
        if self._tag_slug and not session.tagged:
            return
        if session.telemetry_task and not session.telemetry_task.done():
            return
        session.last_gpu_activity = time.time()
        task = asyncio.create_task(self._watch_session(session), name=f"runpod-telemetry-{session.user_id}")
        session.telemetry_task = task

        def _done(t: asyncio.Task) -> None:
            with contextlib.suppress(asyncio.CancelledError):
                exc = t.exception()
                if exc:
                    logger.error("Telemetry task for %s failed: %s", session.user_id, exc)

        task.add_done_callback(_done)

    def _build_payload(self, user_id: str) -> Dict[str, Any]:
        args: Dict[str, Any] = {
            "name": self._compose_pod_name(user_id),
            "cloud_type": self.cloud_type or "ALL",
            "start_ssh": True,
        }
        if self.template_id:
            args["template_id"] = self.template_id
        if self.image_name:
            args["image_name"] = self.image_name
        if self.gpu_type_id:
            args["gpu_type_id"] = self.gpu_type_id
            if self.gpu_count:
                args["gpu_count"] = self.gpu_count
        elif self.gpu_count:
            args["gpu_count"] = self.gpu_count
        if self.volume_gb:
            args["volume_in_gb"] = self.volume_gb
        if self.region:
            args["country_code"] = self.region
        args.setdefault("support_public_ip", True)

        if self.extra_payload:
            allowed = {
                "name",
                "image_name",
                "gpu_type_id",
                "cloud_type",
                "support_public_ip",
                "start_ssh",
                "data_center_id",
                "country_code",
                "gpu_count",
                "volume_in_gb",
                "container_disk_in_gb",
                "min_vcpu_count",
                "min_memory_in_gb",
                "docker_args",
                "ports",
                "volume_mount_path",
                "env",
                "template_id",
                "network_volume_id",
                "allowed_cuda_versions",
                "min_download",
                "min_upload",
                "instance_id",
            }
            camel_to_snake = {
                "templateId": "template_id",
                "imageName": "image_name",
                "cloudType": "cloud_type",
                "supportPublicIp": "support_public_ip",
                "startSsh": "start_ssh",
                "dataCenterId": "data_center_id",
                "countryCode": "country_code",
                "gpuTypeId": "gpu_type_id",
                "gpuCount": "gpu_count",
                "volumeInGb": "volume_in_gb",
                "containerDiskInGb": "container_disk_in_gb",
                "minVcpuCount": "min_vcpu_count",
                "minMemoryInGb": "min_memory_in_gb",
                "dockerArgs": "docker_args",
                "ports": "ports",
                "volumeMountPath": "volume_mount_path",
                "networkVolumeId": "network_volume_id",
                "allowedCudaVersions": "allowed_cuda_versions",
                "minDownload": "min_download",
                "minUpload": "min_upload",
                "instanceId": "instance_id",
            }
            for key, value in self.extra_payload.items():
                snake_key = camel_to_snake.get(key, key)
                if snake_key in allowed:
                    args[snake_key] = value
        return args

    def _record_prompt(self, session: RunpodSession, timestamp: float) -> None:
        dq = session.prompt_history
        if self.prompt_threshold_window > 0:
            cutoff = timestamp - self.prompt_threshold_window
            while dq and dq[0] < cutoff:
                dq.popleft()
        else:
            dq.clear()
        dq.append(timestamp)

    def _should_start_provision(self, session: RunpodSession) -> bool:
        if self.prompt_threshold_count <= 0:
            return True
        return len(session.prompt_history) >= self.prompt_threshold_count

    def _compose_pod_name(self, user_id: str) -> str:
        tag = self._tag_slug or "comfy"
        user_slug = _slugify(user_id) or "user"
        max_len = 60
        remaining = max_len - len(tag) - 1
        if remaining <= 0:
            return tag[:max_len]
        user_part = user_slug[:remaining] or "user"
        return f"{tag}-{user_part}"

    def _pod_has_tag(self, pod_name: Optional[str]) -> bool:
        if not self._tag_slug:
            return True
        if not pod_name:
            return False
        return pod_name.lower().startswith(f"{self._tag_slug}-")

    def _can_retry(self, session: RunpodSession, now: float) -> bool:
        return (now - session.last_state_change) >= self.failure_backoff

    def _reset_for_retry(self, session: RunpodSession) -> None:
        session.base_url = None
        session.pod_id = None
        session.error = None
        if session.telemetry_task and not session.telemetry_task.done():
            session.telemetry_task.cancel()
        session.telemetry_task = None
        session.tagged = False
        session.state = "pending"
        session.last_state_change = time.time()
        session.last_gpu_activity = time.time()

    async def _provision_session(self, session: RunpodSession) -> None:
        if self._closing:
            return
        logger.info("Provisioning RunPod session for %s", session.user_id)
        session.state = "creating"
        session.last_state_change = time.time()
        try:
            payload = self._build_payload(session.user_id)
            created = await self.client.create_pod(payload)
            pod_id = created.get("id")
            if not pod_id:
                raise RuntimeError("RunPod create response missing 'id'")
            session.tagged = self._pod_has_tag(created.get("name") or payload.get("name"))
            session.pod_id = pod_id
            session.state = "starting"
            session.last_state_change = time.time()
            await self._wait_for_pod_ready(session)
            session.base_url = self._format_proxy_url(pod_id)
            await self._wait_for_http_ready(session)
            await self._wait_for_telemetry_ready(session)
            session.state = "ready"
            session.ready_at = time.time()
            session.last_state_change = time.time()
            self._ensure_telemetry_task(session)
            logger.info("RunPod session ready for %s -> %s", session.user_id, session.base_url)
        except Exception as exc:
            session.error = str(exc)
            session.failure_count += 1
            session.state = "failed"
            session.last_state_change = time.time()
            logger.error("RunPod provisioning failed for %s: %s", session.user_id, exc)
            await self.client.safe_terminate(session.pod_id)
            session.pod_id = None
            session.base_url = None
        finally:
            session.provision_task = None

    async def _wait_for_pod_ready(self, session: RunpodSession) -> None:
        assert session.pod_id, "pod_id must be set before waiting"
        deadline = time.time() + self.startup_timeout
        while True:
            pod = await self.client.get_pod(session.pod_id)
            session.tagged = self._pod_has_tag(pod.get("name"))
            status = (pod.get("desiredStatus") or pod.get("status") or "").upper()
            if status in READY_STATUSES:
                return
            if status in FAILURE_STATUSES:
                raise RuntimeError(f"RunPod pod {session.pod_id} entered failure state {status}")
            if time.time() > deadline:
                raise TimeoutError(f"RunPod pod {session.pod_id} did not become ready within {self.startup_timeout}s")
            await asyncio.sleep(self.poll_interval)

    async def _wait_for_http_ready(self, session: RunpodSession) -> None:
        assert session.base_url, "base_url must be set before HTTP health"
        deadline = time.time() + self.startup_timeout
        url = f"{session.base_url}{self.health_path}"
        while True:
            try:
                resp = await self._health_client.get(url)
                if 200 <= resp.status_code < 300:
                    return
            except httpx.HTTPError as exc:
                logger.debug("Health check failed for %s: %s", url, exc)
            if time.time() > deadline:
                raise TimeoutError(f"RunPod base {url} failed health within {self.startup_timeout}s")
            await asyncio.sleep(self.poll_interval)

    def _format_proxy_url(self, pod_id: str) -> str:
        return self.proxy_template.format(pod_id=pod_id, port=self.proxy_port).rstrip("/")

    def _format_telemetry_url(self, pod_id: str) -> Optional[str]:
        if not self.telemetry_template:
            return None
        port = self.telemetry_port if self.telemetry_port is not None else self.proxy_port
        return self.telemetry_template.format(pod_id=pod_id, port=port).rstrip("/")

    async def _wait_for_telemetry_ready(self, session: RunpodSession) -> None:
        if not self._telemetry_client or not self.telemetry_template or not session.pod_id:
            return
        base = self._format_telemetry_url(session.pod_id)
        if not base:
            return
        url = f"{base}{self.telemetry_path}"
        deadline = time.time() + self.startup_timeout
        while True:
            try:
                resp = await self._telemetry_client.get(url)
                if 200 <= resp.status_code < 300:
                    return
            except Exception as exc:
                logger.debug("Telemetry readiness failed for %s: %s", url, exc)
            if time.time() > deadline:
                raise TimeoutError(f"Telemetry endpoint {url} failed readiness within {self.startup_timeout}s")
            await asyncio.sleep(self.poll_interval)

    async def _watch_session(self, session: RunpodSession) -> None:
        if not self._telemetry_client:
            return
        if self._tag_slug and not session.tagged:
            return
        user_id = session.user_id
        while not self._closing:
            if session.state != "ready" or not session.pod_id:
                return
            base = self._format_telemetry_url(session.pod_id)
            if not base:
                return
            metrics_url = f"{base}{self.telemetry_path}"
            got_metrics = False
            try:
                resp = await self._telemetry_client.get(metrics_url)
                if resp.status_code == 200:
                    got_metrics = True
                    data = resp.json()
                    if self._gpu_active(data):
                        session.last_gpu_activity = time.time()
                else:
                    logger.debug("Telemetry check for %s returned %s", metrics_url, resp.status_code)
            except Exception as exc:
                logger.debug("Telemetry fetch failed for %s: %s", metrics_url, exc)
            if got_metrics:
                idle = time.time() - session.last_gpu_activity
                if idle >= self.telemetry_idle_timeout:
                    logger.info(
                        "Terminating RunPod session for %s due to GPU idle (%.0fs)",
                        user_id,
                        idle,
                    )
                    session.telemetry_task = None
                    await self._terminate_session(user_id, reason="telemetry-idle")
                    return
            await asyncio.sleep(self.telemetry_poll_interval)

    def _gpu_active(self, telemetry: Dict[str, Any]) -> bool:
        gpus = telemetry.get("gpus")
        if not isinstance(gpus, list):
            return False
        for gpu in gpus:
            util = gpu.get("utilization_gpu_percent")
            if util is None:
                continue
            try:
                if float(util) > self.telemetry_gpu_threshold:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    async def _reaper(self) -> None:
        try:
            while not self._closing:
                await asyncio.sleep(self.poll_interval)
                cutoff = time.time() - self.idle_timeout
                async with self._lock:
                    stale = [user_id for user_id, sess in self._sessions.items() if sess.last_activity < cutoff]
                for user_id in stale:
                    await self._terminate_session(user_id, reason="idle-timeout")
        except asyncio.CancelledError:
            pass


__all__ = [
    "RunpodClient",
    "RunpodSession",
    "RunpodSessionManager",
    "UpstreamTarget",
]
