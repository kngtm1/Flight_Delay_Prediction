# Flight_Delay_Prediction

# Install Docker
- sudo apt-get update
- sudo apt-get install -y docker.io
- sudo systemctl start docker
- sudo usermod -aG docker ubuntu
- newgrp docker

# Clone and build
- git clone https://github.com/kngtm1/Flight_Delay_Prediction.git
- cd Flight_Delay_Prediction
- docker build -t flight-delay .
-  docker run -p 8501:8501 flight-delay
