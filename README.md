# Flight_Delay_Prediction
We deployed our app on a Docker Container sitting in an EC2 Instance. 

# Set up EC2 Instance
- Use a large size instance with at least 8GB of memory, using the Ubunutu AMI
- For the security group, add 8501 to the inbound rules
- No need to attach a volume, but resize the nvme0n1 (root) to at least 25 GB
- Launch

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
