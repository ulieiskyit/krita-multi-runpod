import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from src.gateway import create_app, GatewayConfig
import httpx

# Mock httpx.AsyncClient for the app's internal calls
class MockAsyncClient:
    async def request(self, *args, **kwargs):
        return MagicMock(status_code=200, headers={}, content=b"{}")
    async def get(self, *args, **kwargs):
        return MagicMock(status_code=200, headers={}, content=b"{}")
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass

@pytest.mark.asyncio
async def test_gateway_routing_ip_fallback():
    # Setup
    config = GatewayConfig(
        duty_pod_url="http://duty",
        use_client_ip=True,
        require_user_id=False
    )
    session_manager = MagicMock()
    session_manager.resolve = AsyncMock()
    session_manager.start = AsyncMock()
    session_manager.shutdown = AsyncMock()
    session_manager.register_prompt = AsyncMock()
    session_manager.bump_activity = AsyncMock()
    
    app = create_app(config, session_manager)
    
    # We mock the internal httpx client used by the gateway to talk to pods
    with patch("httpx.AsyncClient", return_value=MockAsyncClient()):
        
        # 1. Simulate Prompt Request (POST /prompt) with client_id
        client_id = "507badcc-1c7d-42c5-bc63-a1946006f500"
        client_ip = "127.0.0.1"
        body = f'{{"client_id": "{client_id}", "prompt": {{}}}}'.encode()
        
        # Use ASGITransport to mock client IP
        transport = httpx.ASGITransport(app=app, client=(client_ip, 12345))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/prompt",
                content=body,
                headers={"Content-Type": "application/json"}
            )
            assert response.status_code == 200
            
            # Verify session_manager.resolve was called with the UUID
            session_manager.resolve.assert_called_with(client_id)
            
            # Verify mapping is updated
            assert app.state.ip_to_user.get(client_ip) == client_id
            
            # 2. Send Image Request (GET /api/etn/image/...) WITHOUT client_id
            session_manager.resolve.reset_mock()
            
            response = await client.get("/api/etn/image/test.png")
            
            # Verify session_manager.resolve was called with the UUID, NOT the IP
            session_manager.resolve.assert_called_with(client_id)
            
            # 3. Send Request from NEW IP
            new_ip = "192.168.1.1"
            session_manager.resolve.reset_mock()
            
            # New transport for new IP
            transport_new = httpx.ASGITransport(app=app, client=(new_ip, 12345))
            async with httpx.AsyncClient(transport=transport_new, base_url="http://test") as client_new:
                response = await client_new.get("/api/etn/image/other.png")
                
                # Verify session_manager.resolve was called with the IP
                session_manager.resolve.assert_called_with(new_ip)
