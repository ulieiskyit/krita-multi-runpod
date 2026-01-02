import sys
import os
import asyncio
import uuid
import random
import time
from typing import NamedTuple

# Add krita-ai-diffusion to path
krita_plugin_path = os.path.join(os.getcwd(), "krita-ai-diffusion")
if krita_plugin_path not in sys.path:
    sys.path.append(krita_plugin_path)

try:
    from PyQt5.QtCore import QCoreApplication
    from ai_diffusion import eventloop
    from ai_diffusion.comfy_client import ComfyClient
    from ai_diffusion.api import WorkflowInput, WorkflowKind, CheckpointInput, ImageInput, SamplingInput, ConditioningInput
    from ai_diffusion.image import Extent
    from ai_diffusion.client import ClientEvent
except ImportError as e:
    print(f"Error importing dependencies: {e}")
    sys.exit(1)

class QtTestApp:
    def __init__(self):
        self._app = QCoreApplication.instance() or QCoreApplication([])
        eventloop.setup()

    def run(self, coro):
        task = eventloop.run(coro)
        while not task.done():
            self._app.processEvents()
        return task.result()

async def simulate_user(user_index, url):
    user_id = str(uuid.uuid4())
    print(f"[User {user_index}] Starting with ID: {user_id}")
    
    try:
        # Create client and override ID
        client = await ComfyClient.connect(url)
        client._id = user_id # OVERRIDE CONFIG ID
        
        # We need to re-establish websocket with new ID? 
        # Actually ComfyClient connects websocket in .connect() using the ID from settings.
        # So we create the client FIRST, then we might need to reconnect or just manually instantiate?
        # ComfyClient.connect() is a static method that instantiates and connects.
        # To strictly use a different ID, we should instantiate ComfyClient manually and then call internal connect logic? 
        # OR, since ComfyClient.connect() calls Websockets with `client._id`, if we change it *after* object creation but *before* websocket connection...
        # Wait, ComfyClient.connect() does everything. 
        # Hack: connect first (using default ID), then disconnect, change ID, and connect again? No.
        # Better Hack: Instantiate ComfyClient manually, then call connect logic myself? 
        # Let's try to just use valid individual instances. 
        # Wait, ComfyClient.connect passes `client._id` to the websocket URL.
        # `client = ComfyClient(parse_url(url))` happens inside `connect`.
        # `client._id` is set in `__init__`.
        
        # So, to use a custom ID, we should Instantiate manually.
        from ai_diffusion.comfy_client import parse_url, websocket_url, websocket_args, _list_languages, _find_text_encoder_models, _find_vae_models, _find_control_models, _find_clip_vision_model, _find_ip_adapters, _find_style_models, _find_upscalers, _find_inpaint_models, _find_loras, _check_for_missing_nodes, DeviceInfo, ComfyObjectInfo, MissingResources, ClientFeatures, websockets, settings
        
        # Helper to do what ComfyClient.connect does but with custom ID
        client = ComfyClient(parse_url(url))
        client._id = user_id # Custom ID
        
        # Reproduce connect logic
        # Retrieve system info
        client.device_info = DeviceInfo.parse(await client._get("system_stats"))
        
        # Websocket
        wsurl = websocket_url(client.url)
        # We need to start the listener manually or just do what connect does... 
        # actually connect() enters a context manager? No.
        # It uses `async with websockets.connect(...)`.
        # Wait, the valid way to keep connection open is `client.listen()` which is called later.
        # `connect()` just validates the connection and models.
        
        # Let's modify ComfyClient.connect to accept an optional ID? No, can't modify source easily.
        # We'll just replicate the minimal setup needed.
        
        nodes = ComfyObjectInfo(await client._get("object_info"))
        # Skip extensive checks for load test speed, just get checkpoints
        client._features = ClientFeatures(ip_adapter=True, translation=True, gguf=False)
        
        # Refresh models
        checkpoints = {}
        async for status, result in client.try_inspect("checkpoints"):
             checkpoints.update(result)
        client._refresh_models(nodes, checkpoints, None)
        
        if not client.models.checkpoints:
             print(f"[User {user_index}] No checkpoints found.")
             return False

        # User requested specific models
        REQUESTED_MODELS = [
            "AlbedoBase_XL.safetensors",
            "DreamShaperXL_Lightning.safetensors",
            "RealCartoon-XL.safetensors"
        ]
        
        # Pick model round-robin
        wanted_model = REQUESTED_MODELS[user_index % len(REQUESTED_MODELS)]
        checkpoint_name = wanted_model
        
        # Check if available, otherwise find best match or fallback
        available = list(client.models.checkpoints.keys())
        found = False
        for name in available:
            if wanted_model.lower() in name.lower():
                checkpoint_name = name
                found = True
                break
        
        if not found:
            print(f"[User {user_index}] Warn: Requested {wanted_model} not found. Available: {available[:3]}...")
            # Fallback to first
            checkpoint_name = available[0]

        print(f"[User {user_index}] Using {checkpoint_name}")
        
        # INJECT USER ID HEADER to ensure Gateway sees unique users
        if hasattr(client, '_session'):
            client._session.headers.update({"X-Comfy-User": user_id})
        
        workflow = WorkflowInput(
            WorkflowKind.generate,
            models=CheckpointInput(checkpoint_name),
            images=ImageInput.from_extent(Extent(512, 512)),
            conditioning=ConditioningInput(f"Test prompt from user {user_index}", "bad quality"),
            sampling=SamplingInput("euler", "normal", cfg_scale=7.0, total_steps=10),
        )

        job_done = asyncio.Event()
        jobs_completed = 0
        connection_count = 0
        TARGET_JOBS = 5 # Threshold is 2, so 5 should trigger and verify switch
        
        async def run_workflow():
            nonlocal jobs_completed, connection_count
            start_time = time.time()
            
            # This consumes the messages
            async for msg in client.listen():
                if msg.event == ClientEvent.connected:
                     connection_count += 1
                     print(f"[User {user_index}] Connected (Count: {connection_count})")
                     
                     if connection_count > 1:
                         print(f"[User {user_index}] !!! DETECTED RECONNECTION/SWITCH !!!")
                     
                     # Only start enqueuing if this is the first connection, OR if we need to re-queue?
                     # Actually, we rely on the loop below to enqueue based on completed jobs.
                     # But for the FIRST connection we need to kick it off.
                     if connection_count == 1:
                         print(f"[User {user_index}] Initial connection. Sending first batch...")
                         await client.enqueue(workflow)
                         print(f"[User {user_index}] Enqueued job 1/{TARGET_JOBS}")

                elif msg.event == ClientEvent.progress:
                     pass 
                elif msg.event == ClientEvent.finished:
                     jobs_completed += 1
                     elapsed = time.time() - start_time
                     print(f"[User {user_index}] Finished job {jobs_completed}/{TARGET_JOBS} (Time since start: {elapsed:.1f}s)")
                     
                     if jobs_completed < TARGET_JOBS:
                        # Enqueue next
                        await client.enqueue(workflow)
                        print(f"[User {user_index}] Enqueued job {jobs_completed + 1}/{TARGET_JOBS}")
                     else:
                        print(f"[User {user_index}] All jobs done.")
                        job_done.set()
                        break
                        
                elif msg.event == ClientEvent.error:
                     print(f"[User {user_index}] Error: {msg.error}")
                     # If error is about network, client might reconnect. 
                     # If it's a generation error, we might want to stop or retry.
                     # For load test, let's stop.
                     job_done.set()
                     break

        # Run the listener/handler with a timeout
        try:
             # Allow ample time for provisioning (which might take minutes)
             await asyncio.wait_for(run_workflow(), timeout=600)
        except asyncio.TimeoutError:
             print(f"[User {user_index}] Timed out!")
        
        await client.disconnect()
        return jobs_completed >= TARGET_JOBS

    except Exception as e:
        print(f"[User {user_index}] Exception: {e}")
        import traceback; traceback.print_exc()
        return False

async def main_load_test(num_users=3):
    url = "http://127.0.0.1:8000"
    print(f"Starting load test with {num_users} concurrent users against {url}")
    print(f"Goal: Trigger pod switch (Threshold=2) and use different models.")
    
    start_time = time.time()
    
    # Create tasks
    tasks = [simulate_user(i, url) for i in range(num_users)]
    results = await asyncio.gather(*tasks)
    
    duration = time.time() - start_time
    success_count = sum(1 for r in results if r)
    
    print("\n--- Load Test Results ---")
    print(f"Total Users: {num_users}")
    print(f"Successful Sessions: {success_count}")
    print(f"Failed: {num_users - success_count}")
    print(f"Total Duration: {duration:.2f}s")
    
    return success_count == num_users

if __name__ == "__main__":
    app = QtTestApp()
    # Read num_users from args if available
    num_users = 3
    if len(sys.argv) > 1:
        try:
            num_users = int(sys.argv[1])
        except:
            pass
            
    success = app.run(main_load_test(num_users))
    if not success:
        sys.exit(1)
