FROM python:3.10

# System dependencies needed for some Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    liblzma-dev \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Upgrade pip first
RUN pip install --upgrade pip

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501
ENV PYTHONUNBUFFERED=1

CMD ["./start.sh"]
