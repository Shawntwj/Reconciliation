# 1. Use a slim version of Python 3.13 for a smaller image footprint
FROM python:3.13-slim

# 2. Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files to disk
# PYTHONUNBUFFERED: Ensures logs are sent straight to terminal (crucial for Docker logs)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 3. Install system dependencies (required for psycopg2 to compile/run)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 4. Install Python dependencies
# We copy this first to leverage Docker's layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy application code and data
# Note: In production, you'd use a volume for data, but for an assignment, 
# copying the CSV into the image ensures it runs "out of the box".
COPY src/ ./src/
COPY trades.csv .
COPY .env .env

# 6. Create a non-root user for security
RUN useradd -m myuser
USER myuser

# 7. Default Command
# Since this is a data pipeline, you might want to run ingestion first, 
# then start the API.
CMD ["sh", "-c", "python src/ingest.py && uvicorn src.api:app --host 0.0.0.0 --port 8000"]