FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (for layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create persistent directories
RUN mkdir -p data logs

# Defaults for container environment
ENV PYTHONUNBUFFERED=1
ENV TRADING_MODE=paper
ENV HEADLESS=true
ENV DASHBOARD_ENABLED=true
ENV DASHBOARD_PORT=8080

EXPOSE 8080

CMD ["python", "-m", "main"]
