#!/bin/bash

# 1. Run the modelling script
echo "Running modelling.py..."
python modelling.py

# 2. Start Streamlit app
echo "Starting Streamlit..."
streamlit run streamlit.py --server.address=0.0.0.0
