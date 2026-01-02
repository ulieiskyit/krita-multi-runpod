import sys
import os
import asyncio
from typing import NamedTuple

# Add krita-ai-diffusion to path so we can import ai_diffusion
krita_plugin_path = os.path.join(os.getcwd(), "krita-ai-diffusion")
if krita_plugin_path not in sys.path:
    sys.path.append(krita_plugin_path)

try:
    from PyQt5.QtCore import QCoreApplication
    from ai_diffusion import eventloop
    from ai_diffusion.comfy_client import ComfyClient
except ImportError as e:
    print(f"Error importing dependencies: {e}")
    print("Please install requirements: pip install -r krita-ai-diffusion/requirements.txt")
    print("And ensure submodules are updated: git submodule update --init --recursive")
    sys.exit(1)

class QtTestApp:
    def __init__(self):
        self._app = QCoreApplication([])
        eventloop.setup()

    def run(self, coro):
        task = eventloop.run(coro)
        while not task.done():
            self._app.processEvents()
        return task.result()

from ai_diffusion.api import WorkflowInput, WorkflowKind, CheckpointInput, ImageInput, SamplingInput, ConditioningInput
from ai_diffusion.image import Extent
from ai_diffusion.client import ClientEvent, resolve_arch
from ai_diffusion.style import Arch

# ... existing code ...

async def test_generation():
    url = "http://127.0.0.1:8000"
    print(f"Connecting to {url}...")
    try:
        client = await ComfyClient.connect(url)
        print("Connected successfully!")
        
        # Discover models to ensure we have one
        async for _ in client.discover_models(refresh=True):
            pass

        print(f"Checkpoints: {len(client.models.checkpoints)}")
        if not client.models.checkpoints:
            print("No checkpoints found! Cannot run generation test.")
            return False

        # Pick the first available checkpoint
        # We try to find a known supported one, or just take the first one
        checkpoint_name = list(client.models.checkpoints.keys())[0]
        for name in client.models.checkpoints.keys():
            if "sd15" in name.lower() or "v1-5" in name.lower():
                checkpoint_name = name
                break
        
        print(f"Using checkpoint: {checkpoint_name}")
        
        # Create a simple generation workflow
        # 512x512, 20 steps, Euler Ancestral (or similar)
        # Using the Arch from the checkpoint info if possible
        cp_info = client.models.checkpoints[checkpoint_name]
        
        workflow = WorkflowInput(
            WorkflowKind.generate,
            models=CheckpointInput(checkpoint_name),
            images=ImageInput.from_extent(Extent(512, 512)),
            conditioning=ConditioningInput("a beautiful landscape with mountains and a lake, detailed, 8k", "bad quality, blurry"),
            sampling=SamplingInput("euler", "normal", cfg_scale=7.0, total_steps=20),
        )
        
        print("Enqueuing job...")
        job_id = await client.enqueue(workflow)
        print(f"Job ID: {job_id}")

        print("Listening for updates...")
        async for msg in client.listen():
            if msg.event == ClientEvent.progress:
                print(f"Progress: {msg.progress:.0%}")
            elif msg.event == ClientEvent.finished:
                print("Finished!")
                if msg.images:
                    print(f"Received {len(msg.images)} image(s)")
                    for img in msg.images:
                        print(f"  Dimension: {img.width}x{img.height}")
                    # In a real test we might save it, but here just confirming receipt is enough
                else:
                    print("Finished but no images received?")
                break
            elif msg.event == ClientEvent.error:
                print(f"Error: {msg.error}")
                return False
            elif msg.event == ClientEvent.interrupted:
                print("Interrupted!")
                return False
        
        await client.disconnect()
        return True

    except Exception as e:
        print(f"\nGeneration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main_test():
    # Run connection test first (optional, but good for sanity)
    # await test_connection() 
    return await test_generation()

if __name__ == "__main__":
    app = QtTestApp()
    print("Running generation integration test...")
    success = app.run(main_test())
    if not success:
        sys.exit(1)
