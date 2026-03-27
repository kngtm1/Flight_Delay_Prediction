#!/bin/bash

# Check model exists before starting
if [ ! -f "model.json" ]; then
    echo "ERROR: model.json not found. Please include it in the build context."
    exit 1
fi

echo "Starting Streamlit..."
streamlit run app.py \
    --server.address=0.0.0.0 \
    --server.port=8501 \
    --server.headless=true
```
