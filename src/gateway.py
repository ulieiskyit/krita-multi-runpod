"""FastAPI gateway that fronts ComfyUI pods."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import parse_qsl, urlencode

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.websockets import WebSocketState

from src.runpod_manager import RunpodSessionManager, UpstreamTarget

logger = logging.getLogger("comfy.gateway")


class PromptQueueState:
    """Serializes prompt submissions per user and releases once execution ends."""

    def __init__(self) -> None:
        self._gate = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._active_prompt_id: Optional[str] = None

    async def wait_turn(self) -> None:
        await self._gate.acquire()

    async def prompt_started(self, prompt_id: Optional[str]) -> None:
        async with self._state_lock:
            self._active_prompt_id = prompt_id
        if prompt_id is None:
            self._release_if_locked()

    async def prompt_finished(self, prompt_id: Optional[str]) -> None:
        async with self._state_lock:
            if self._active_prompt_id and prompt_id and prompt_id != self._active_prompt_id:
                return
            self._active_prompt_id = None
        self._release_if_locked()

    async def prompt_failed(self) -> None:
        async with self._state_lock:
            self._active_prompt_id = None
        self._release_if_locked()

    async def is_active(self, prompt_id: Optional[str] = None) -> bool:
        async with self._state_lock:
            if prompt_id is None:
                return self._active_prompt_id is not None
            return self._active_prompt_id == prompt_id

    def _release_if_locked(self) -> None:
        if self._gate.locked():
            self._gate.release()


HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
}

STRIP_ENTITY_HEADERS = {"content-length", "content-encoding"}


def _filter_headers(h: httpx.Headers | Dict[str, str]) -> Dict[str, str]:
    filtered = {}
    for k, v in dict(h).items():
        lower = k.lower()
        if lower in HOP_BY_HOP or lower in STRIP_ENTITY_HEADERS:
            continue
        filtered[k] = v
    return filtered


@dataclass
class GatewayConfig:
    duty_pod_url: str
    health_path: str = "/queue"
    health_timeout_s: float = 10.0
    user_id_header: str = "X-Comfy-User"
    user_id_query_key: str = "user_id"
    user_id_cookie: Optional[str] = None
    require_user_id: bool = True
    use_client_id: bool = True
    use_client_ip: bool = False


def create_app(config: GatewayConfig, session_manager: Optional[RunpodSessionManager]) -> FastAPI:
    app = FastAPI(title="Comfy Gateway (multi-user RunPod)")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    duty_url = config.duty_pod_url.rstrip("/")
    runpod_enabled = session_manager is not None
    app.state.session_manager = session_manager
    app.state.config = config
    app.state.prompt_queues: Dict[str, PromptQueueState] = {}
    app.state.ip_to_user: Dict[str, str] = {}

    @app.on_event("startup")
    async def _startup() -> None:
        if session_manager:
            await session_manager.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if session_manager:
            await session_manager.shutdown()

    def _extract_user_id_from_request(request: Request) -> Optional[str]:
        header_val = request.headers.get(config.user_id_header)
        if header_val:
            return header_val.strip()
        if config.user_id_cookie:
            cookie_val = request.cookies.get(config.user_id_cookie)
            if cookie_val:
                return cookie_val.strip()
        query_val = request.query_params.get(config.user_id_query_key)
        if query_val:
            return query_val.strip()
        if config.use_client_id:
            for key in ("client_id", "clientId"):
                if key in request.query_params:
                    val = request.query_params.get(key)
                    if val:
                        return val.strip()
        
        if request.client and request.client.host:
            mapped = app.state.ip_to_user.get(request.client.host)
            if mapped:
                return mapped

        if config.use_client_ip and request.client:
            host = getattr(request.client, "host", None)
            if host:
                return host
        return None

    def _extract_user_id_from_ws(ws: WebSocket, query: Dict[str, str]) -> Optional[str]:
        header_val = ws.headers.get(config.user_id_header)
        if header_val:
            return header_val.strip()
        if config.user_id_cookie:
            cookie_header = ws.headers.get("cookie")
            if cookie_header:
                cookies = dict(pair.split("=", 1) for pair in cookie_header.split("; ") if "=" in pair)
                if config.user_id_cookie in cookies:
                    return cookies[config.user_id_cookie].strip()
        if config.user_id_query_key in query and query[config.user_id_query_key]:
            return query[config.user_id_query_key].strip()
        if config.use_client_id:
            for key in ("client_id", "clientId"):
                if key in query and query[key]:
                    return query[key].strip()
        if config.use_client_ip and ws.client:
            host = getattr(ws.client, "host", None)
            if host:
                return host
        return None

    def _get_prompt_queue(user_id: Optional[str]) -> PromptQueueState:
        key = user_id.strip() if user_id else "__anon__"
        queues: Dict[str, PromptQueueState] = app.state.prompt_queues
        queue = queues.get(key)
        if queue is None:
            queue = PromptQueueState()
            queues[key] = queue
        return queue

    def _is_prompt_request(path: str, request: Request) -> bool:
        if request.method != "POST":
            return False
        normalized = path.lower().rstrip("/")
        return normalized.endswith("prompt")

    def _extract_client_id_from_body(body: Optional[bytes]) -> Optional[str]:
        if not body:
            return None
        try:
            payload = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(payload, dict):
            return None
        for key in ("client_id", "clientId"):
            value = payload.get(key)
            if isinstance(value, str):
                trimmed = value.strip()
                if trimmed:
                    return trimmed
        return None

    async def _resolve_for_request(request: Request, body: Optional[bytes] = None) -> UpstreamTarget:
        user_id = _extract_user_id_from_request(request)
        if _is_prompt_request(request.url.path, request):
            if body is None:
                body = await request.body()
            client_id = _extract_client_id_from_body(body)
            if client_id:
                user_id = client_id
        
        if user_id and request.client and request.client.host:
            if user_id != request.client.host:
                app.state.ip_to_user[request.client.host] = user_id

        if not user_id:
            if config.require_user_id:
                logger.warning(
                    "Rejecting %s %s: missing user identifier",
                    request.method,
                    request.url.path,
                )
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Missing user identifier. Provide header "
                        f"{config.user_id_header} or query parameter {config.user_id_query_key}."
                    ),
                )
            logger.info(
                "Routing %s %s to duty pod (missing user id)",
                request.method,
                request.url.path,
            )
            return UpstreamTarget(url=duty_url, session_ready=False, via_duty=True)
        if not runpod_enabled:
            logger.debug(
                "RunPod disabled; routing %s %s to duty pod for user %s",
                request.method,
                request.url.path,
                user_id,
            )
            return UpstreamTarget(url=duty_url, session_ready=False, via_duty=True, user_id=user_id)
        assert session_manager is not None
        target = await session_manager.resolve(user_id)
        logger.debug(
            "Routing %s %s to %s pod for user %s",
            request.method,
            request.url.path,
            "duty" if target.via_duty else "private",
            user_id,
        )
        return target

    def _attach_gateway_headers(response: Response, target: UpstreamTarget) -> Response:
        if not isinstance(response, Response):
            return response
        response.headers["X-Comfy-Session-Ready"] = "1" if target.session_ready else "0"
        response.headers["X-Comfy-Session-Source"] = "private" if not target.via_duty else "duty"
        if target.user_id:
            response.headers["X-Comfy-Session-User"] = target.user_id
        return response

    # ------------------------- Light assets -------------------------

    @app.get("/user.css")
    async def user_css_root() -> Response:
        return PlainTextResponse("", media_type="text/css")

    @app.get("/favicon.ico")
    async def favicon() -> Response:
        return Response(status_code=204)

    # --------------------- Userdata pass-through ---------------------

    @app.api_route("/api/userdata", methods=["GET"])
    async def userdata_index(request: Request):
        target = await _resolve_for_request(request)
        base = target.url
        qs = request.url.query
        upstream_url = f"{base}/api/userdata" + (f"?{qs}" if qs else "")
        headers = _filter_headers(request.headers)

        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(upstream_url, headers=headers)
            if r.status_code != 404:
                content_type = r.headers.get("content-type", "")
                hdrs = _filter_headers(r.headers)
                if "application/json" in content_type:
                    response = JSONResponse(status_code=r.status_code, content=r.json(), headers=hdrs)
                elif "text/" in content_type or content_type == "":
                    response = PlainTextResponse(status_code=r.status_code, content=r.text, headers=hdrs)
                else:
                    response = Response(
                        content=r.content,
                        status_code=r.status_code,
                        headers=hdrs,
                        media_type=content_type or None,
                    )
                return _attach_gateway_headers(response, target)
        except Exception:
            pass

        response = JSONResponse(status_code=200, content=[])
        return _attach_gateway_headers(response, target)

    @app.get("/api/userdata/user.css")
    async def userdata_user_css(request: Request):
        target = await _resolve_for_request(request)
        base = target.url
        url = f"{base}/api/userdata/user.css"
        try:
            async with httpx.AsyncClient(timeout=5) as cli:
                r = await cli.get(url)
            if r.status_code == 200:
                response = PlainTextResponse(r.text, media_type=r.headers.get("content-type", "text/css"))
                return _attach_gateway_headers(response, target)
        except Exception:
            pass
        response = PlainTextResponse("", media_type="text/css")
        return _attach_gateway_headers(response, target)

    # ------------------------- Health endpoints -------------------------

    @app.get("/system_stats")
    async def system_stats(request: Request):
        target = await _resolve_for_request(request)
        base = target.url
        url = f"{base}/system_stats"
        try:
            logger.debug("Checking system_stats from %s", url)
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(url)
                if r.status_code == 200:
                    if r.headers.get("content-type", "").startswith("application/json"):
                        response = JSONResponse(status_code=200, content=r.json())
                    else:
                        response = PlainTextResponse(status_code=200, content=r.text)
                    return _attach_gateway_headers(response, target)
        except Exception as exc:
            logger.warning("system_stats request failed: %s", exc)
            pass

        queue_info = None
        try:
            async with httpx.AsyncClient(timeout=5) as cli:
                rq = await cli.get(f"{base}/queue")
                if rq.status_code == 200 and rq.headers.get("content-type", "").startswith("application/json"):
                    queue_info = rq.json()
        except Exception:
            pass

        response = JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "gateway_fallback": True,
                "upstream": base,
                "queue": queue_info,
            },
        )
        return _attach_gateway_headers(response, target)

    @app.get("/")
    async def root() -> Dict[str, bool | str]:
        return {"ok": True, "service": "comfy-gateway-runpod"}

    @app.get("/object_info")
    async def object_info(request: Request):
        target = await _resolve_for_request(request)
        base = target.url
        url = f"{base}/object_info"
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(url)
                if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/json"):
                    response = JSONResponse(status_code=200, content=r.json())
                    return _attach_gateway_headers(response, target)
        except Exception:
            pass

        response = JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "gateway_fallback": True,
                "upstream": base,
                "info": {},
            },
        )
        return _attach_gateway_headers(response, target)

    # ----------------------------- WebSocket -----------------------------

    @app.websocket("/ws")
    async def ws_proxy(ws: WebSocket):
        await ws.accept()

        raw_q = ws.scope.get("query_string", b"").decode()
        query = dict(parse_qsl(raw_q, keep_blank_values=True))
        if "clientId" in query and "client_id" not in query:
            query["client_id"] = query["clientId"]
        if "client_id" in query and "clientId" not in query:
            query["clientId"] = query["client_id"]

        user_id = _extract_user_id_from_ws(ws, query)
        if not user_id and config.require_user_id:
            await ws.close(code=4401, reason=f"Missing user identifier header {config.user_id_header}")
            return

        if user_id and runpod_enabled and session_manager:
            current_target = await session_manager.resolve(user_id)
        else:
            current_target = UpstreamTarget(
                url=duty_url,
                session_ready=bool(user_id and runpod_enabled),
                via_duty=not (user_id and runpod_enabled),
                user_id=user_id,
            )

        queue_state = _get_prompt_queue(user_id) if user_id or not config.require_user_id else None

        subprotocols = ws.scope.get("subprotocols") or None

        async def _resolve_target() -> UpstreamTarget:
            if user_id and runpod_enabled and session_manager:
                return await session_manager.resolve(user_id)
            return UpstreamTarget(
                url=duty_url,
                session_ready=bool(user_id and runpod_enabled),
                via_duty=not (user_id and runpod_enabled),
                user_id=user_id,
            )

        async def _watch_for_switch(current_url: str) -> Optional[UpstreamTarget]:
            if not (user_id and runpod_enabled and session_manager):
                return None
            while True:
                await asyncio.sleep(1.0)
                try:
                    new_target = await session_manager.resolve(user_id)
                except Exception as exc:  # pragma: no cover - runtime warning
                    logging.getLogger("uvicorn.error").warning(
                        "[GW] WS resolve failed for %s: %s", user_id, exc
                    )
                    continue
                logging.getLogger("uvicorn.error").debug(
                    "[GW] WS monitor target=%s ready=%s via_duty=%s",
                    new_target.url,
                    new_target.session_ready,
                    new_target.via_duty,
                )
                if (
                    new_target.session_ready
                    and not new_target.via_duty
                    and new_target.url.rstrip("/") != current_url.rstrip("/")
                ):
                    logging.getLogger("uvicorn.error").info(
                        "[GW] WS switching %s -> %s", current_url, new_target.url
                    )
                    return new_target

        current_target = await _resolve_target()
        logging.getLogger("uvicorn.error").debug(
            "[GW] WS initial target=%s ready=%s via_duty=%s",
            current_target.url,
            current_target.session_ready,
            current_target.via_duty,
        )

        async def _pump_client(upstream):
            while True:
                msg = await ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    raise WebSocketDisconnect
                if "text" in msg:
                    await upstream.send(msg["text"])
                elif "bytes" in msg:
                    await upstream.send(msg["bytes"])
                if user_id and session_manager:
                    await session_manager.bump_activity(user_id)

        async def _handle_upstream_message(payload: str) -> None:
            if not queue_state:
                return
            try:
                data = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                return
            if not isinstance(data, dict):
                return
            msg_type = data.get("type")
            if msg_type in {"execution_end", "execution_success", "execution_error", "execution_interrupted"}:
                prompt_data = data.get("data") or {}
                prompt_id = prompt_data.get("prompt_id")
                if isinstance(prompt_id, str) and prompt_id:
                    await queue_state.prompt_finished(prompt_id)
                else:
                    await queue_state.prompt_finished(None)
                return
            if msg_type == "status":
                status = (data.get("data") or {}).get("status") or {}
                exec_info = status.get("exec_info") or {}
                queue_pending = exec_info.get("queue_pending")
                queue_remaining = exec_info.get("queue_remaining")
                queue_running = exec_info.get("queue_running")
                if all(
                    val in {0, None}
                    for val in (queue_pending, queue_remaining, queue_running)
                ):
                    await queue_state.prompt_finished(None)

        async def _pump_upstream(upstream):
            while True:
                msg = await upstream.recv()
                if isinstance(msg, bytes):
                    await ws.send_bytes(msg)
                else:
                    await ws.send_text(msg)
                    await _handle_upstream_message(msg)
                if user_id and session_manager:
                    await session_manager.bump_activity(user_id)

        async def _drain_task(task: asyncio.Task) -> None:
            with contextlib.suppress(
                asyncio.CancelledError,
                WebSocketDisconnect,
                websockets.exceptions.ConnectionClosed,
                Exception,
            ):
                await task

        while True:
            base = current_target.url
            upstream_url = f"{base.replace('http', 'ws')}/ws"
            if query:
                upstream_url += f"?{urlencode(query)}"

            try:
                logging.getLogger("uvicorn.error").info(f"[GW] WS connect -> {upstream_url}")
                async with websockets.connect(
                    upstream_url,
                    ping_interval=None,
                    max_size=None,
                    subprotocols=subprotocols,
                ) as upstream:
                    client_task = asyncio.create_task(_pump_client(upstream))
                    upstream_task = asyncio.create_task(_pump_upstream(upstream))
                    switch_task: Optional[asyncio.Task] = None
                    if user_id and runpod_enabled and session_manager:
                        switch_task = asyncio.create_task(_watch_for_switch(base))

                    tasks = [client_task, upstream_task]
                    if switch_task:
                        tasks.append(switch_task)

                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                    next_target: Optional[UpstreamTarget] = None
                    if switch_task and switch_task in done:
                        next_target = switch_task.result()

                    if next_target:
                        for task in (client_task, upstream_task):
                            task.cancel()
                            await _drain_task(task)
                        with contextlib.suppress(Exception):
                            await upstream.close(code=1012, reason="Switching upstream")
                        if switch_task and switch_task not in done:
                            switch_task.cancel()
                        if switch_task:
                            await _drain_task(switch_task)
                        logging.getLogger("uvicorn.error").info(
                            "[GW] WS reconnecting to %s", next_target.url
                        )
                        current_target = next_target
                        continue

                    # No switch – either client disconnected or upstream closed
                    for task in pending:
                        task.cancel()
                    for task in tasks:
                        await _drain_task(task)
            except WebSocketDisconnect:
                break
            except websockets.exceptions.ConnectionClosedOK:
                break
            except websockets.exceptions.InvalidStatusCode as e:
                logging.getLogger("uvicorn.error").error(
                    f"[GW] WS upstream {upstream_url} failed: {e.status_code} {getattr(e, 'headers', None)}"
                )
                try:
                    await ws.close(code=1011, reason=f"Upstream WS error {e.status_code}")
                except Exception:
                    pass
                break
            except Exception as e:
                logging.getLogger("uvicorn.error").error(f"[GW] WS unexpected error: {e}")
                try:
                    await ws.close(code=1011, reason="Gateway error")
                except Exception:
                    pass
                break

            if (
                current_target.via_duty
                and user_id
                and runpod_enabled
                and session_manager
            ):
                try:
                    latest_target = await session_manager.resolve(user_id)
                except Exception as exc:
                    logging.getLogger("uvicorn.error").warning(
                        "[GW] WS post-close resolve failed for %s: %s", user_id, exc
                    )
                else:
                    if (
                        latest_target.session_ready
                        and not latest_target.via_duty
                        and latest_target.url.rstrip("/") != current_target.url.rstrip("/")
                    ):
                        logging.getLogger("uvicorn.error").info(
                            "[GW] WS reconnecting post-close to %s", latest_target.url
                        )
                        current_target = latest_target
                        continue

            break

        if ws.application_state != WebSocketState.DISCONNECTED:
            try:
                await ws.close()
            except Exception:
                pass

    # ----------------------------- Catch-all -----------------------------

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    async def proxy_all(path: str, request: Request):
        print(f"DEBUG: Request to /{path}")
        body = await request.body()
        target = await _resolve_for_request(request, body)
        headers = _filter_headers(request.headers)
        is_prompt = _is_prompt_request(path, request)

        async def _forward(
            current_target: UpstreamTarget,
            queue_state: Optional[PromptQueueState],
        ) -> tuple[Response, Optional[str]]:
            base = current_target.url
            qs = request.url.query
            upstream_url = f"{base}/{path}" + (f"?{qs}" if qs else "")

            if request.method == "OPTIONS":
                response = Response(status_code=204)
                response = _attach_gateway_headers(response, current_target)
                if queue_state:
                    await queue_state.prompt_started(None)
                return response, None

            try:
                logger.debug("Proxying %s %s to %s", request.method, path, upstream_url)
                async with httpx.AsyncClient(timeout=None) as cli:
                    r = await cli.request(
                        request.method,
                        upstream_url,
                        content=body if request.method in {"POST", "PUT", "PATCH"} else None,
                        headers=headers,
                    )
            except Exception as exc:
                logger.error("Proxy failed for %s: %s", upstream_url, exc)
                if queue_state:
                    await queue_state.prompt_failed()
                raise

            content_type = r.headers.get("content-type", "")
            hdrs = _filter_headers(r.headers)

            json_payload: Optional[Dict[str, object]] = None
            if "application/json" in content_type:
                try:
                    json_payload = r.json()
                except json.JSONDecodeError:
                    json_payload = None

            if json_payload is not None:
                response = JSONResponse(status_code=r.status_code, content=json_payload, headers=hdrs)
            elif "text/" in content_type or content_type == "":
                response = PlainTextResponse(status_code=r.status_code, content=r.text, headers=hdrs)
            else:
                response = Response(
                    content=r.content,
                    status_code=r.status_code,
                    headers=hdrs,
                    media_type=content_type or None,
                )

            response = _attach_gateway_headers(response, current_target)

            prompt_id: Optional[str] = None
            if queue_state and isinstance(json_payload, dict):
                prompt_id_obj = json_payload.get("prompt_id")
                if isinstance(prompt_id_obj, str) and prompt_id_obj:
                    prompt_id = prompt_id_obj
                    response.headers.setdefault("X-Comfy-Prompt-Id", prompt_id)
            if queue_state:
                await queue_state.prompt_started(prompt_id)

            if (
                session_manager
                and current_target.user_id
                and current_target.via_duty
                and is_prompt
            ):
                # Parse the prompt payload to pass to register_prompt
                prompt_payload: Optional[Dict[str, object]] = None
                if body:
                    try:
                        prompt_payload = json.loads(body)
                    except (json.JSONDecodeError, TypeError):
                        pass
                await session_manager.register_prompt(current_target.user_id, prompt_payload)

            return response, prompt_id

        async def _wait_for_prompt_turn(
            queue_state: PromptQueueState,
        ) -> None:
            try:
                await queue_state.wait_turn()
            except Exception:
                await queue_state.prompt_failed()
                raise

        def _prompt_in_payload(payload: object, needle: str) -> bool:
            if isinstance(payload, str):
                return payload == needle
            if isinstance(payload, dict):
                prioritized = []
                fallback = []
                for key, value in payload.items():
                    key_lower = str(key).lower()
                    if any(term in key_lower for term in ("done", "history")):
                        continue
                    if any(term in key_lower for term in ("pending", "running")):
                        prioritized.append(value)
                    else:
                        fallback.append(value)
                for value in prioritized:
                    if _prompt_in_payload(value, needle):
                        return True
                for value in fallback:
                    if _prompt_in_payload(value, needle):
                        return True
                return False
            if isinstance(payload, list):
                return any(_prompt_in_payload(item, needle) for item in payload)
            return False

        async def _watch_prompt_completion(
            base_url: str,
            prompt_id: str,
            queue_state: PromptQueueState,
        ) -> None:
            check_url = f"{base_url}/queue"
            try:
                async with httpx.AsyncClient(timeout=10) as cli:
                    attempts = 0
                    while await queue_state.is_active(prompt_id):
                        attempts += 1
                        if attempts > 1800:  # ~60 minutes of monitoring
                            await queue_state.prompt_finished(prompt_id)
                            return
                        try:
                            resp = await cli.get(check_url)
                        except Exception:
                            await asyncio.sleep(2.0)
                            continue
                        if resp.status_code != 200:
                            await asyncio.sleep(2.0)
                            continue
                        try:
                            data = resp.json()
                        except Exception:
                            await asyncio.sleep(2.0)
                            continue
                        if not _prompt_in_payload(data, prompt_id):
                            await queue_state.prompt_finished(prompt_id)
                            return
                        await asyncio.sleep(2.0)
            except Exception:
                await queue_state.prompt_finished(prompt_id)

        if is_prompt:
            queue_state = _get_prompt_queue(target.user_id)
            await _wait_for_prompt_turn(queue_state)
            try:
                refreshed_target = await _resolve_for_request(request, body)
                response, prompt_id = await _forward(refreshed_target, queue_state)
                if prompt_id:
                    asyncio.create_task(
                        _watch_prompt_completion(refreshed_target.url, prompt_id, queue_state)
                    )
            except Exception:
                await queue_state.prompt_failed()
                raise
        else:
            response, _ = await _forward(target, None)

        return response

    return app


__all__ = ["GatewayConfig", "create_app"]
