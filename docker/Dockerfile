FROM nvidia/cuda:12.8.1-base-ubuntu22.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PATH="/venv/bin:$PATH"

# --- system dependencies ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv python3-pip git ca-certificates curl wget build-essential \
    libglib2.0-0 libsm6 libgl1 libxrender1 libxext6 \
 && rm -rf /var/lib/apt/lists/*

# --- uv + virtualenv ---
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"
RUN uv venv /venv --python 3.11

# --- application requirements ---
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --no-cache -r /tmp/requirements.txt

COPY telemetry_requirements.txt /tmp/telemetry_requirements.txt
RUN uv pip install --no-cache -r /tmp/telemetry_requirements.txt

# --- ComfyUI sources ---
ARG COMFYUI_COMMIT=7d6103325e1c97aa54f963253e3e7f1d6da6947f
ARG IPADAPTER_PLUS_COMMIT=a0f451a5113cf9becb0847b92884cb10cbdec0ef 
ARG INPAINT_NODES_COMMIT=1e2543e5619b3f6e88b7727243c8b9c5791a7b4a
ARG TOOLING_NODES_COMMIT=75c632df4bd0808035b95804e74108d59aac97cd
ARG CONTROLNET_AUX_COMMIT=12f35647f0d510e03b45a47fb420fe1245a575df

WORKDIR /opt
RUN git clone https://github.com/comfyanonymous/ComfyUI.git /ComfyUI && \
    git -C /ComfyUI checkout ${COMFYUI_COMMIT}
RUN mkdir -p /ComfyUI/custom_nodes && \
    git clone https://github.com/cubiq/ComfyUI_IPAdapter_plus.git /ComfyUI/custom_nodes/ComfyUI_IPAdapter_plus && \
    git -C /ComfyUI/custom_nodes/ComfyUI_IPAdapter_plus checkout ${IPADAPTER_PLUS_COMMIT} && \
    git clone https://github.com/Acly/comfyui-inpaint-nodes.git /ComfyUI/custom_nodes/comfyui-inpaint-nodes && \
    git -C /ComfyUI/custom_nodes/comfyui-inpaint-nodes checkout ${INPAINT_NODES_COMMIT} && \
    git clone https://github.com/Acly/comfyui-tooling-nodes.git /ComfyUI/custom_nodes/comfyui-tooling-nodes && \
    git -C /ComfyUI/custom_nodes/comfyui-tooling-nodes checkout ${TOOLING_NODES_COMMIT} && \
    git clone https://github.com/Fannovel16/comfyui_controlnet_aux.git /ComfyUI/custom_nodes/comfyui_controlnet_aux && \
    git -C /ComfyUI/custom_nodes/comfyui_controlnet_aux checkout ${CONTROLNET_AUX_COMMIT}
#RUN uv pip install --no-cache -r /ComfyUI/requirements.txt

# --- optional extra model paths ---
COPY extra_model_paths.yaml /ComfyUI/

# --- telemetry agent ---
COPY telemetry_agent.py /telemetry_agent.py

# --- entrypoint script ---
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 8000 8188
CMD ["/start.sh"]
