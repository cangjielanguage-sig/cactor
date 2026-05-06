# 🚀 CActor Deployment Guide

## 📋 Deployment Overview

CActor v7.0.0 supports a variety of deployment methods, from single-machine development environments to large-scale production clusters, offering flexible deployment options and configuration strategies.

## 🏗️ Deployment Architecture

### Single-Node Deployment
Suitable for development, testing, and small-scale production environments:

```
┌─────────────────────────────────────┐
│           CActor Application        │
├─────────────────────────────────────┤
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ │
│  │ Actor   │ │ Actor   │ │ Actor   │ │
│  │ System  │ │ System  │ │ System  │ │
│  │    A    │ │    B    │ │    C    │ │
│  └─────────┘ └─────────┘ └─────────┘ │
├─────────────────────────────────────┤
│         CActor Runtime              │
├─────────────────────────────────────┤
│         Operating System            │
└─────────────────────────────────────┘
```

### Cluster Deployment
Suitable for large-scale production environments:

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Node 1    │  │   Node 2    │  │   Node 3    │
│             │  │             │  │             │
│ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │
│ │ Shard   │ │  │ │ Shard   │ │  │ │ Shard   │ │
│ │ Region  │ │  │ │ Region  │ │  │ │ Region  │ │
│ │   A     │ │  │ │   B     │ │  │ │   C     │ │
│ └─────────┘ │  │ └─────────┘ │  │ └─────────┘ │
│             │  │             │  │             │
│ CActor      │  │ CActor      │  │ CActor      │
│ Runtime     │  │ Runtime     │  │ Runtime     │
└─────────────┘  └─────────────┘  └─────────────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
              ┌─────────────────┐
              │ Cluster Manager │
              │ (Coordinator)   │
              └─────────────────┘
```

## 🔧 Environment Configuration

### System Requirements

#### Minimum Requirements
- **CPU**: 2 cores
- **Memory**: 4GB RAM
- **Storage**: 10GB available space
- **Network**: 1Gbps
- **Operating System**: Linux/macOS/Windows

#### Recommended Configuration
- **CPU**: 8 cores or more
- **Memory**: 16GB RAM or more
- **Storage**: SSD, 50GB+ available space
- **Network**: 10Gbps
- **Operating System**: Linux (Ubuntu 20.04+, CentOS 8+)

#### High-Performance Configuration
- **CPU**: 16 cores or more
- **Memory**: 64GB RAM or more
- **Storage**: NVMe SSD, 100GB+
- **Network**: 25Gbps or higher
- **Operating System**: Optimized Linux distribution

### Cangjie Language Environment

```bash
# Install Cangjie language (0.60.5+)
# Please refer to the official documentation: https://cangjie-lang.cn/

# Verify installation
cjpm --version
```

## 📦 Build and Packaging

### Development Build

```bash
# Clone the project
git clone https://github.com/your-repo/cactor.git
cd cactor

# Development build
cjpm build

# Run tests
cjpm test
```

### Production Build

```bash
# Production-optimized build
cjpm build --release

# Verify build
./target/release/bin/cactor.examples.hello_world_demo
```

### Packaging and Distribution

```bash
# Create distribution package
mkdir -p dist/cactor-v1.1.0
cp -r target/release/* dist/cactor-v1.1.0/
cp README.md LICENSE dist/cactor-v1.1.0/

# Create compressed archive
tar -czf cactor-v1.1.0.tar.gz -C dist cactor-v1.1.0
```

## 🚀 Single-Node Deployment

### Basic Deployment

```bash
# 1. Extract distribution package
tar -xzf cactor-v1.1.0.tar.gz
cd cactor-v1.1.0

# 2. Configure environment variables
export CACTOR_HOME=$(pwd)
export PATH=$CACTOR_HOME/bin:$PATH

# 3. Create configuration file
cat > config/application.conf << EOF
cactor {
  system {
    name = "ProductionSystem"
    threads = 8
  }

  mailbox {
    capacity = 10000
    batch-size = 100
  }

  logging {
    level = "INFO"
    file = "logs/cactor.log"
  }
}
EOF

# 4. Start the application
./bin/your-application
```

### Service Deployment (systemd)

Create a systemd service file:

```bash
# /etc/systemd/system/cactor-app.service
[Unit]
Description=CActor Application
After=network.target

[Service]
Type=simple
User=cactor
Group=cactor
WorkingDirectory=/opt/cactor
ExecStart=/opt/cactor/bin/your-application
Restart=always
RestartSec=10

Environment=CACTOR_HOME=/opt/cactor
Environment=CACTOR_CONFIG=/opt/cactor/config/application.conf

[Install]
WantedBy=multi-user.target
```

Start the service:

```bash
# Enable and start the service
sudo systemctl enable cactor-app
sudo systemctl start cactor-app

# Check status
sudo systemctl status cactor-app
```

## 🏢 Cluster Deployment

### Cluster Configuration

Configuration file for each node:

```bash
# config/cluster.conf
cactor {
  cluster {
    enabled = true
    node-id = "node-1"  # Must be unique per node

    seeds = [
      "node-1:2551",
      "node-2:2551",
      "node-3:2551"
    ]

    sharding {
      enabled = true
      number-of-shards = 100
      coordinator-singleton-path = "/user/coordinator"
    }
  }

  remote {
    enabled = true
    hostname = "192.168.1.10"  # Node IP
    port = 2551
  }
}
```

### Cluster Startup

```bash
# Node 1
./bin/your-application --config=config/cluster.conf --node-id=node-1

# Node 2
./bin/your-application --config=config/cluster.conf --node-id=node-2

# Node 3
./bin/your-application --config=config/cluster.conf --node-id=node-3
```

### Cluster Management

```bash
# View cluster status
curl http://localhost:8080/cluster/status

# Add a node
curl -X POST http://localhost:8080/cluster/join \
  -d '{"node": "node-4:2551"}'

# Remove a node
curl -X POST http://localhost:8080/cluster/leave \
  -d '{"node": "node-2:2551"}'
```

## 🐳 Containerized Deployment

### Dockerfile

```dockerfile
FROM ubuntu:20.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Cangjie language
RUN wget https://releases.cangjie-lang.cn/cangjie-0.60.5-linux.tar.gz \
    && tar -xzf cangjie-0.60.5-linux.tar.gz \
    && mv cangjie-0.60.5 /opt/cangjie \
    && ln -s /opt/cangjie/bin/cjpm /usr/local/bin/cjpm

# Copy application
COPY target/release /opt/cactor
COPY config /opt/cactor/config

# Set working directory
WORKDIR /opt/cactor

# Expose ports
EXPOSE 2551 8080

# Startup command
CMD ["./bin/your-application"]
```

### Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  cactor-node-1:
    build: .
    hostname: node-1
    ports:
      - "2551:2551"
      - "8080:8080"
    environment:
      - CACTOR_NODE_ID=node-1
      - CACTOR_CLUSTER_SEEDS=node-1:2551,node-2:2551,node-3:2551
    volumes:
      - ./logs:/opt/cactor/logs
      - ./data:/opt/cactor/data

  cactor-node-2:
    build: .
    hostname: node-2
    ports:
      - "2552:2551"
      - "8081:8080"
    environment:
      - CACTOR_NODE_ID=node-2
      - CACTOR_CLUSTER_SEEDS=node-1:2551,node-2:2551,node-3:2551
    volumes:
      - ./logs:/opt/cactor/logs
      - ./data:/opt/cactor/data

  cactor-node-3:
    build: .
    hostname: node-3
    ports:
      - "2553:2551"
      - "8082:8080"
    environment:
      - CACTOR_NODE_ID=node-3
      - CACTOR_CLUSTER_SEEDS=node-1:2551,node-2:2551,node-3:2551
    volumes:
      - ./logs:/opt/cactor/logs
      - ./data:/opt/cactor/data
```

Start the cluster:

```bash
# Start the cluster
docker-compose up -d

# View status
docker-compose ps

# View logs
docker-compose logs -f cactor-node-1
```

## ☸️ Kubernetes Deployment

### Deployment Manifest

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cactor-cluster
  labels:
    app: cactor
spec:
  replicas: 3
  selector:
    matchLabels:
      app: cactor
  template:
    metadata:
      labels:
        app: cactor
    spec:
      containers:
      - name: cactor
        image: cactor:v1.1.0
        ports:
        - containerPort: 2551
        - containerPort: 8080
        env:
        - name: CACTOR_CLUSTER_ENABLED
          value: "true"
        - name: CACTOR_CLUSTER_SEEDS
          value: "cactor-service:2551"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
---
apiVersion: v1
kind: Service
metadata:
  name: cactor-service
spec:
  selector:
    app: cactor
  ports:
  - name: cluster
    port: 2551
    targetPort: 2551
  - name: http
    port: 8080
    targetPort: 8080
  type: ClusterIP
```

Deploy to Kubernetes:

```bash
# Deploy the application
kubectl apply -f k8s/deployment.yaml

# View status
kubectl get pods -l app=cactor
kubectl get services

# Scale up/down
kubectl scale deployment cactor-cluster --replicas=5
```

## 📊 Monitoring and Health Checks

### Health Check Endpoints

```bash
# Application health check
curl http://localhost:8080/health

# Cluster status check
curl http://localhost:8080/cluster/health

# Performance metrics
curl http://localhost:8080/metrics
```

### Monitoring Integration

```yaml
# prometheus configuration
scrape_configs:
  - job_name: 'cactor'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

## 🔧 Configuration Management

### Environment Variables

```bash
# Core configuration
export CACTOR_SYSTEM_NAME="ProductionSystem"
export CACTOR_THREADS=8
export CACTOR_LOG_LEVEL="INFO"

# Cluster configuration
export CACTOR_CLUSTER_ENABLED=true
export CACTOR_NODE_ID="node-1"
export CACTOR_CLUSTER_SEEDS="node-1:2551,node-2:2551"

# Performance configuration
export CACTOR_MAILBOX_CAPACITY=10000
export CACTOR_BATCH_SIZE=100
```

### Configuration File Template

```hocon
# config/production.conf
cactor {
  system {
    name = ${CACTOR_SYSTEM_NAME}
    threads = ${CACTOR_THREADS}
  }

  cluster {
    enabled = ${CACTOR_CLUSTER_ENABLED}
    node-id = ${CACTOR_NODE_ID}
    seeds = ${CACTOR_CLUSTER_SEEDS}
  }

  performance {
    mailbox-capacity = ${CACTOR_MAILBOX_CAPACITY}
    batch-size = ${CACTOR_BATCH_SIZE}
  }
}
```

## 🚨 Fault Recovery

### Automatic Restart Policy

```bash
# systemd automatic restart
Restart=always
RestartSec=10
StartLimitInterval=60
StartLimitBurst=3
```

### Data Backup

```bash
# Backup script
#!/bin/bash
BACKUP_DIR="/backup/cactor/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# Backup configuration
cp -r /opt/cactor/config $BACKUP_DIR/

# Backup data
cp -r /opt/cactor/data $BACKUP_DIR/

# Backup logs
cp -r /opt/cactor/logs $BACKUP_DIR/
```

---

**CActor v7.0.0 - Enterprise Deployment, Production Ready!**

*Last Updated: May 6, 2026*