# CActor 部署指南

## 部署概览

CActor v1.2.0 支持多种部署方式，从单机开发环境到大规模生产集群，提供灵活的部署选项和配置策略。

## 部署架构

### 单机部署

适用于开发、测试和小规模生产环境：

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

### 集群部署

适用于大规模生产环境：

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

## 环境配置

### 系统要求

#### 最低要求

- **CPU**: 2核心
- **内存**: 4GB RAM
- **存储**: 10GB 可用空间
- **网络**: 1Gbps
- **操作系统**: Linux/macOS/Windows

#### 推荐配置

- **CPU**: 8核心或更多
- **内存**: 16GB RAM或更多
- **存储**: SSD，50GB+可用空间
- **网络**: 10Gbps
- **操作系统**: Linux (Ubuntu 20.04+, CentOS 8+)

#### 高性能配置

- **CPU**: 16核心或更多
- **内存**: 64GB RAM或更多
- **存储**: NVMe SSD，100GB+
- **网络**: 25Gbps或更高
- **操作系统**: 优化的Linux发行版

### 仓颉语言环境

```bash
# 安装仓颉语言 (1.2.0+)
# 请参考官方文档: https://cangjie-lang.cn/

# 验证安装
cjpm --version
```

## 构建和打包

### 开发构建

```bash
# 克隆项目
git clone https://github.com/your-repo/cactor.git
cd cactor

# 开发构建
cjpm build

# 运行测试
cjpm test
```

### 生产构建

```bash
# 生产优化构建
cjpm build --release

# 验证构建
./target/release/bin/cactor_examples.hello_world_demo
```

### 打包分发

```bash
# 创建分发包
mkdir -p dist/cactor-v1.2.0
cp -r target/release/* dist/cactor-v1.2.0/
cp README.md LICENSE dist/cactor-v1.2.0/

# 创建压缩包
tar -czf cactor-v1.2.0.tar.gz -C dist cactor-v1.2.0
```

## 单机部署

### 基础部署

```bash
# 1. 解压分发包
tar -xzf cactor-v1.2.0.tar.gz
cd cactor-v1.2.0

# 2. 配置环境变量
export CACTOR_HOME=$(pwd)
export PATH=$CACTOR_HOME/bin:$PATH

# 3. 创建配置文件
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

# 4. 启动应用
./bin/your-application
```

### 服务化部署

创建systemd服务文件：

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

启动服务：

```bash
# 启用并启动服务
sudo systemctl enable cactor-app
sudo systemctl start cactor-app

# 检查状态
sudo systemctl status cactor-app
```

## 集群部署

### 集群配置

每个节点的配置文件：

```bash
# config/cluster.conf
cactor {
  cluster {
    enabled = true
    node-id = "node-1"  # 每个节点唯一

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
    hostname = "192.168.1.10"  # 节点IP
    port = 2551
  }
}
```

### 集群启动

```bash
# 节点1
./bin/your-application --config=config/cluster.conf --node-id=node-1

# 节点2
./bin/your-application --config=config/cluster.conf --node-id=node-2

# 节点3
./bin/your-application --config=config/cluster.conf --node-id=node-3
```

### 集群管理

```bash
# 查看集群状态
curl http://localhost:8080/cluster/status

# 添加节点
curl -X POST http://localhost:8080/cluster/join \
  -d '{"node": "node-4:2551"}'

# 移除节点
curl -X POST http://localhost:8080/cluster/leave \
  -d '{"node": "node-2:2551"}'
```

## 容器化部署

### Dockerfile

```dockerfile
FROM ubuntu:20.04

# 安装依赖
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# 安装仓颉语言
RUN wget https://releases.cangjie-lang.cn/cangjie-1.2.0-linux.tar.gz \
    && tar -xzf cangjie-1.2.0-linux.tar.gz \
    && mv cangjie-1.2.0 /opt/cangjie \
    && ln -s /opt/cangjie/bin/cjpm /usr/local/bin/cjpm

# 复制应用
COPY target/release /opt/cactor
COPY config /opt/cactor/config

# 设置工作目录
WORKDIR /opt/cactor

# 暴露端口
EXPOSE 2551 8080

# 启动命令
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

启动集群：

```bash
# 启动集群
docker-compose up -d

# 查看状态
docker-compose ps

# 查看日志
docker-compose logs -f cactor-node-1
```

## Kubernetes部署

### 部署清单

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
        image: cactor:v1.2.0
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

部署到Kubernetes：

```bash
# 部署应用
kubectl apply -f k8s/deployment.yaml

# 查看状态
kubectl get pods -l app=cactor
kubectl get services

# 扩缩容
kubectl scale deployment cactor-cluster --replicas=5
```

## 监控和健康检查

### 健康检查端点

```bash
# 应用健康检查
curl http://localhost:8080/health

# 集群状态检查
curl http://localhost:8080/cluster/health

# 性能指标
curl http://localhost:8080/metrics
```

### 监控集成

```yaml
# prometheus配置
scrape_configs:
  - job_name: 'cactor'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

## 配置管理

### 环境变量

```bash
# 核心配置
export CACTOR_SYSTEM_NAME="ProductionSystem"
export CACTOR_THREADS=8
export CACTOR_LOG_LEVEL="INFO"

# 集群配置
export CACTOR_CLUSTER_ENABLED=true
export CACTOR_NODE_ID="node-1"
export CACTOR_CLUSTER_SEEDS="node-1:2551,node-2:2551"

# 性能配置
export CACTOR_MAILBOX_CAPACITY=10000
export CACTOR_BATCH_SIZE=100
```

### 配置文件模板

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

## 故障恢复

### 自动重启策略

```bash
# systemd自动重启
Restart=always
RestartSec=10
StartLimitInterval=60
StartLimitBurst=3
```

### 数据备份

```bash
# 备份脚本
#!/bin/bash
BACKUP_DIR="/backup/cactor/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# 备份配置
cp -r /opt/cactor/config $BACKUP_DIR/

# 备份数据
cp -r /opt/cactor/data $BACKUP_DIR/

# 备份日志
cp -r /opt/cactor/logs $BACKUP_DIR/
```

---

**CActor v1.2.0 - 企业级部署，生产就绪！**

*最后更新: 2026年5月6日*