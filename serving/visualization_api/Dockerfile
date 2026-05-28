FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/ais/serving/visualization_api

COPY serving/visualization_api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY serving/visualization_api/ ./

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${VIS_API_PORT:-8080}"]
