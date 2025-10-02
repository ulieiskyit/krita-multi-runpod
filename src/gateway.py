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
        if not user_id:
            if config.require_user_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Missing user identifier. Provide header "
                        f"{config.user_id_header} or query parameter {config.user_id_query_key}."
                    ),
                )
            return UpstreamTarget(url=duty_url, session_ready=False, via_duty=True)
        if not runpod_enabled:
            return UpstreamTarget(url=duty_url, session_ready=False, via_duty=True, user_id=user_id)
        assert session_manager is not None
        return await session_manager.resolve(user_id)

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
                    response = StreamingResponse(r.aiter_raw(), status_code=r.status_code, headers=hdrs)
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
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(url)
                if r.status_code == 200:
                    if r.headers.get("content-type", "").startswith("application/json"):
                        response = JSONResponse(status_code=200, content=r.json())
                    else:
                        response = PlainTextResponse(status_code=200, content=r.text)
                    return _attach_gateway_headers(response, target)
        except Exception:
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

        async def _pump_upstream(upstream):
            while True:
                msg = await upstream.recv()
                if isinstance(msg, bytes):
                    await ws.send_bytes(msg)
                else:
                    await ws.send_text(msg)
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
        body = await request.body()
        target = await _resolve_for_request(request, body)
        base = target.url
        qs = request.url.query
        upstream_url = f"{base}/{path}" + (f"?{qs}" if qs else "")
        headers = _filter_headers(request.headers)

        if request.method == "OPTIONS":
            response = Response(status_code=204)
            return _attach_gateway_headers(response, target)

        async with httpx.AsyncClient(timeout=None) as cli:
            r = await cli.request(
                request.method,
                upstream_url,
                content=body if request.method in {"POST", "PUT", "PATCH"} else None,
                headers=headers,
            )

        content_type = r.headers.get("content-type", "")
        hdrs = _filter_headers(r.headers)

        if "application/json" in content_type:
            response = JSONResponse(status_code=r.status_code, content=r.json(), headers=hdrs)
        elif "text/" in content_type or content_type == "":
            response = PlainTextResponse(status_code=r.status_code, content=r.text, headers=hdrs)
        else:
            response = StreamingResponse(r.aiter_raw(), status_code=r.status_code, headers=hdrs)

        response = _attach_gateway_headers(response, target)

        if (
            session_manager
            and target.user_id
            and target.via_duty
            and _is_prompt_request(path, request)
        ):
            await session_manager.register_prompt(target.user_id)

        return response

    return app


__all__ = ["GatewayConfig", "create_app"]
