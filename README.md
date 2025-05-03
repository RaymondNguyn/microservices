# Microservices Project with Kafka Queue

A distributed system featuring microservice architecture with Kafka message queue, secure credential storage, and automated deployment using Ansible.

## Project Structure

### Services
- **analyzer**
- **anomaly**
- **config/dev**
- **consistency**
- **data**
- **frontend**
- **processing** - Changes to base_path to allow for reverse proxy with nginx
- **receiver** - Consistency service changes
- **storage** - Consistency service changes

### Configuration Files
- **.gitignore** - Changed prod and dev configurations
- **docker-compose.dev.yml**
- **docker-compose.prod.yml**
- **docker-compose.yml**
- **kafka-entrypoint.sh**
- **requirements.txt**

## System Features

- Microservices architecture for scalability and maintainability
- Kafka message queue for reliable asynchronous communication
- Secure credential storage system
- Automated setup with Ansible
- Docker containerization for consistent deployment environments
- Development and production configuration separation



## Development Setup

### Prerequisites
- Docker and Docker Compose
- Kafka
- Ansible (for automated deployment)

### Getting Started
```bash
# For development environment
docker-compose -f docker-compose.yml docker-compose.dev.yml up

# For production environment
docker-compose -f docker-compose.yml docker-compose.prod.yml up
```

### Service Communication Architecture
```
┌────────────┐    ┌────────────┐    ┌────────────┐
│  Frontend  │━━━▶│  Receiver  │━━━▶│    Kafka   │
└────────────┘    └────────────┘    └────────────┘
                                          ┃
                                          ▼
┌────────────┐    ┌────────────┐    ┌────────────┐
│  Storage   │◀━━━│ Processing │◀━━━│  Analyzer  │
└────────────┘    └────────────┘    └────────────┘
                                          ┃
                                          ▼
┌────────────┐    ┌────────────┐    ┌────────────┐
│    Data    │◀━━━│  Anomaly   │◀━━━│Consistency │
└────────────┘    └────────────┘    └────────────┘
```

## Deployment
The system utilizes Ansible for automated deployment, allowing for consistent and reproducible infrastructure setup across different environments.

```bash
# Deploy using docker-compose
docker-compose -f docker-compose docker-compose.prod.yml up -d
```
