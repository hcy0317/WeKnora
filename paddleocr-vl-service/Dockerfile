FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PADDLE_PDX_MODEL_SOURCE=BOS

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl libgl1 libglib2.0-0 libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install paddlepaddle==3.2.1 \
        -i https://www.paddlepaddle.org.cn/packages/stable/cpu/ \
    && python -m pip install -U "paddleocr[doc-parser]" \
    && paddlex --install serving

EXPOSE 8080
CMD ["paddlex", "--serve", "--pipeline", "PaddleOCR-VL-1.6", "--device", "cpu", "--host", "0.0.0.0", "--port", "8080"]
