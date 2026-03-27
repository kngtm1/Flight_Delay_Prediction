FROM python:3.10

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose Streamlit port
EXPOSE 8501

# Avoid Python buffering issues
ENV PYTHONUNBUFFERED=1

# Run wrapper script
CMD ["./start.sh"]
