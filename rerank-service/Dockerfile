FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=1000 \
    PIP_RETRIES=20

RUN python -m pip install \
        "https://mirrors.aliyun.com/pytorch-wheels/cpu/torch-2.7.1%2Bcpu-cp312-cp312-manylinux_2_28_x86_64.whl" \
    && python -m pip install "fastapi>=0.116,<1" "uvicorn>=0.35,<1" "sentence-transformers[onnx]>=5.1,<6"

WORKDIR /srv
COPY server.py /srv/server.py

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
