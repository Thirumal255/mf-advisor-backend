FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Expose port
EXPOSE 8080

# Create tmp directories for Cloud Run
RUN mkdir -p /tmp/data /tmp/faiss_index

# Run the application
# 🟢 CHANGE YOUR LAST LINE TO THIS:
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}