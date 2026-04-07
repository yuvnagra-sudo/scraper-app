FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Railway sets PORT env var
ENV PORT=8080
ENV JOBS_DIR=/data/jobs

# Persistent storage mount point
RUN mkdir -p /data/jobs

EXPOSE 8080

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
