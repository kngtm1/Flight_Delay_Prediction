FROM python:3.10-slim

# System dependencies
RUN apt-get update && apt-get install -y \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV TMPDIR=/data/docker/tmp

COPY requirements.txt .

# Upgrade pip and install dependencies (CPU only, prefer binary wheels)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefer-binary -r requirements.txt

COPY . .

EXPOSE 8501
ENV PYTHONUNBUFFERED=1

RUN chmod +x start.sh
CMD ["./start.sh"]
