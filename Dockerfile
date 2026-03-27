FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /data/docker/tmp
ENV TMPDIR=/data/docker/tmp
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefer-binary -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "streamlit.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
