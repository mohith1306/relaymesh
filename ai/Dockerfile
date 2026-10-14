FROM python:3.12-slim

WORKDIR /app

COPY ai/requirements.txt ./ai/requirements.txt
RUN pip install --no-cache-dir -r ai/requirements.txt

COPY ai/ ./ai/

ENV PORT=50051
EXPOSE 50051

CMD ["python", "-m", "ai.service.server"]
