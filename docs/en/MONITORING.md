# CActor Monitoring and Logging Guide

## Monitoring Overview

CActor v1.2.0 provides comprehensive monitoring and logging capabilities to help you gain real-time insight into system status, performance metrics, and potential issues, ensuring stable operation in production environments.

## Monitoring Architecture

### Monitoring Hierarchy

```
+-------------------------------------------------------------+
|                    Application Layer Monitoring              |
|  +---------------+ +---------------+ +---------------+      |
|  | Business KPIs | | User Behavior | | Error Rate    |      |
|  +---------------+ +---------------+ +---------------+      |
+-------------------------------------------------------------+
|                    Actor Layer Monitoring                   |
|  +---------------+ +---------------+ +---------------+      |
|  | Message       | | Mailbox       | | Actor         |      |
|  | Throughput    | | Status        | | Lifecycle     |      |
|  +---------------+ +---------------+ +---------------+      |
+-------------------------------------------------------------+
|                    System Layer Monitoring                  |
|  +---------------+ +---------------+ +---------------+      |
|  | CPU Usage     | | Memory Usage  | | Network I/O   |      |
|  +---------------+ +---------------+ +---------------+      |
+-------------------------------------------------------------+
|                    Infrastructure Monitoring                |
|  +---------------+ +---------------+ +---------------+      |
|  | Disk I/O      | | Network Delay | | Service Health|      |
|  +---------------+ +---------------+ +---------------+      |
+-------------------------------------------------------------+
```

## Core Monitoring Metrics

### 1. Performance Metrics

```cangjie
// Performance metrics collector
public struct PerformanceMetrics {
    public let messageProcessingRate: Float64      // Message processing rate (msg/s)
    public let averageLatency: Duration           // Average latency
    public let p95Latency: Duration                // P95 latency
    public let p99Latency: Duration                // P99 latency
    public let throughput: Int64                   // Throughput
    public let errorRate: Float64                  // Error rate
    public let cpuUsage: Float64                   // CPU usage
    public let memoryUsage: Int64                  // Memory usage
    public let activeActors: Int64                 // Active actor count
    public let mailboxSize: Int64                  // Mailbox size
}

class MetricsCollector <: Actor {
    private var metrics: PerformanceMetrics = PerformanceMetrics.empty()
    private let metricsHistory: CircularBuffer<PerformanceMetrics> = CircularBuffer(1000)

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case collect: CollectMetricsMessage =>
                metrics = collectCurrentMetrics()
                metricsHistory.add(metrics)
                context.sender.tell(MetricsResponse(metrics))
                MessageResult.Handled

            case query: QueryMetricsMessage =>
                let historicalMetrics = getHistoricalMetrics(query.timeRange)
                context.sender.tell(HistoricalMetricsResponse(historicalMetrics))
                MessageResult.Handled
        }
    }

    private func collectCurrentMetrics(): PerformanceMetrics {
        return PerformanceMetrics(
            messageProcessingRate: calculateMessageRate(),
            averageLatency: calculateAverageLatency(),
            p95Latency: calculateP95Latency(),
            p99Latency: calculateP99Latency(),
            throughput: calculateThroughput(),
            errorRate: calculateErrorRate(),
            cpuUsage: SystemMetrics.getCpuUsage(),
            memoryUsage: SystemMetrics.getMemoryUsage(),
            activeActors: ActorRegistry.getActiveActorCount(),
            mailboxSize: calculateAverageMailboxSize()
        )
    }
}
```

### 2. Actor Monitoring

```cangjie
// Actor monitoring decorator
class MonitoredActor <: Actor {
    private let wrappedActor: Actor
    private var messageCount: Int64 = 0
    private var totalProcessingTime: Duration = Duration.zero()
    private var errorCount: Int64 = 0
    private let startTime: DateTime = DateTime.now()

    public init(wrappedActor: Actor) {
        this.wrappedActor = wrappedActor
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let processingStart = System.nanoTime()
        messageCount += 1

        try {
            let result = wrappedActor.receive(message, context)

            let processingEnd = System.nanoTime()
            let processingTime = Duration.fromNanos(processingEnd - processingStart)
            totalProcessingTime += processingTime

            // Record performance metrics
            recordMetrics(message, processingTime, result)

            return result
        } catch (e: Exception) {
            errorCount += 1
            recordError(message, e)
            throw e
        }
    }

    private func recordMetrics(message: Message, processingTime: Duration, result: MessageResult): Unit {
        let metrics = ActorMetrics(
            actorPath: self.path,
            messageType: message.getClass().getSimpleName(),
            processingTime: processingTime,
            result: result,
            timestamp: DateTime.now()
        )

        metricsCollector.tell(RecordMetricsMessage(metrics))
    }

    public func getStatistics(): ActorStatistics {
        let uptime = DateTime.now().minus(startTime)
        let avgProcessingTime = if (messageCount > 0) {
            totalProcessingTime.dividedBy(messageCount)
        } else {
            Duration.zero()
        }

        return ActorStatistics(
            messageCount: messageCount,
            errorCount: errorCount,
            errorRate: if (messageCount > 0) { errorCount.toFloat64() / messageCount.toFloat64() } else { 0.0 },
            averageProcessingTime: avgProcessingTime,
            uptime: uptime
        )
    }
}
```

### 3. Cluster Monitoring

```cangjie
// Cluster health monitoring
class ClusterHealthMonitor <: Actor {
    private var clusterState: ClusterState = ClusterState.empty()
    private let healthCheckInterval: Duration = Duration.fromSeconds(30)

    public func preStart(): Unit {
        // Periodic health checks
        context.system.scheduler.scheduleWithFixedDelay(
            Duration.zero(),
            healthCheckInterval,
            self,
            PerformHealthCheckMessage()
        )
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: PerformHealthCheckMessage =>
                performHealthCheck()
                MessageResult.Handled

            case query: ClusterStatusQuery =>
                context.sender.tell(ClusterStatusResponse(clusterState))
                MessageResult.Handled

            case event: ClusterEvent =>
                updateClusterState(event)
                MessageResult.Handled
        }
    }

    private func performHealthCheck(): Unit {
        let nodes = cluster.getNodes()
        let healthyNodes = ArrayList<NodeInfo>()
        let unhealthyNodes = ArrayList<NodeInfo>()

        for (node in nodes) {
            if (isNodeHealthy(node)) {
                healthyNodes.add(node)
            } else {
                unhealthyNodes.add(node)
                // Send alert
                alertingService.sendAlert(NodeUnhealthyAlert(node))
            }
        }

        clusterState = ClusterState(
            totalNodes: nodes.size(),
            healthyNodes: healthyNodes.size(),
            unhealthyNodes: unhealthyNodes.size(),
            nodeDetails: nodes,
            lastUpdate: DateTime.now()
        )
    }

    private func isNodeHealthy(node: NodeInfo): Bool {
        try {
            let response = httpClient.get("http://${node.address}/health", Duration.fromSeconds(5))
            return response.status == 200
        } catch (e: Exception) {
            return false
        }
    }
}
```

## Logging System

### 1. Structured Logging

```cangjie
// Structured logger
public enum LogLevel {
    | Debug
    | Info
    | Warn
    | Error
    | Fatal
}

public struct LogEntry {
    public let level: LogLevel
    public let timestamp: DateTime
    public let actorPath: String
    public let message: String
    public let context: HashMap<String, String>
    public let exception: Option<Exception>

    public func toJson(): String {
        return JsonBuilder()
            .add("level", level.toString())
            .add("timestamp", timestamp.toISOString())
            .add("actor", actorPath)
            .add("message", message)
            .add("context", context)
            .add("exception", exception.map { it.toString() })
            .build()
    }
}

class StructuredLogger {
    private let logLevel: LogLevel
    private let logWriter: LogWriter

    public func log(level: LogLevel, message: String, context: HashMap<String, String> = HashMap(), exception: Option<Exception> = None): Unit {
        if (level.ordinal() >= logLevel.ordinal()) {
            let entry = LogEntry(
                level: level,
                timestamp: DateTime.now(),
                actorPath: getCurrentActorPath(),
                message: message,
                context: context,
                exception: exception
            )

            logWriter.write(entry)
        }
    }

    public func debug(message: String, context: HashMap<String, String> = HashMap()): Unit {
        log(LogLevel.Debug, message, context)
    }

    public func info(message: String, context: HashMap<String, String> = HashMap()): Unit {
        log(LogLevel.Info, message, context)
    }

    public func warn(message: String, context: HashMap<String, String> = HashMap()): Unit {
        log(LogLevel.Warn, message, context)
    }

    public func error(message: String, exception: Option<Exception> = None, context: HashMap<String, String> = HashMap()): Unit {
        log(LogLevel.Error, message, context, exception)
    }
}
```

### 2. Log Aggregation

```cangjie
// Log aggregation actor
class LogAggregator <: Actor {
    private let logBuffer: CircularBuffer<LogEntry> = CircularBuffer(10000)
    private let logFilters: ArrayList<LogFilter> = ArrayList()
    private let logForwarders: ArrayList<LogForwarder> = ArrayList()

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case log: LogMessage =>
                processLogEntry(log.entry)
                MessageResult.Handled

            case query: LogQueryMessage =>
                let results = queryLogs(query.criteria)
                context.sender.tell(LogQueryResponse(results))
                MessageResult.Handled

            case config: LogConfigMessage =>
                updateConfiguration(config)
                MessageResult.Handled
        }
    }

    private func processLogEntry(entry: LogEntry): Unit {
        // Apply filters
        if (logFilters.all { filter => filter.accept(entry) }) {
            logBuffer.add(entry)

            // Forward to external systems
            for (forwarder in logForwarders) {
                forwarder.forward(entry)
            }

            // Check alert conditions
            checkAlertConditions(entry)
        }
    }

    private func checkAlertConditions(entry: LogEntry): Unit {
        if (entry.level == LogLevel.Error || entry.level == LogLevel.Fatal) {
            alertingService.sendAlert(ErrorLogAlert(entry))
        }

        // Check error rate threshold
        let recentErrors = logBuffer.filter { log =>
            log.level == LogLevel.Error &&
            DateTime.now().minus(log.timestamp) < Duration.fromMinutes(5)
        }.size()

        if (recentErrors > 100) {
            alertingService.sendAlert(HighErrorRateAlert(recentErrors))
        }
    }
}
```

## Monitoring Integration

### 1. Prometheus Integration

```cangjie
// Prometheus metrics exporter
class PrometheusMetricsExporter <: Actor {
    private let metricsRegistry: MetricsRegistry = MetricsRegistry()
    private let httpServer: HttpServer = HttpServer(8080)

    public func preStart(): Unit {
        // Register core metrics
        registerCoreMetrics()

        // Start HTTP server
        httpServer.route("/metrics", { request => handleMetricsRequest(request) })
        httpServer.start()
    }

    private func registerCoreMetrics(): Unit {
        metricsRegistry.registerGauge("cactor_active_actors", { => getActiveActorCount() })
        metricsRegistry.registerCounter("cactor_messages_processed_total")
        metricsRegistry.registerHistogram("cactor_message_processing_duration_seconds")
        metricsRegistry.registerGauge("cactor_mailbox_size", { => getAverageMailboxSize() })
        metricsRegistry.registerGauge("cactor_memory_usage_bytes", { => getMemoryUsage() })
        metricsRegistry.registerGauge("cactor_cpu_usage_percent", { => getCpuUsage() })
    }

    private func handleMetricsRequest(request: HttpRequest): HttpResponse {
        let metricsText = metricsRegistry.exportPrometheusFormat()
        return HttpResponse.ok(metricsText, "text/plain")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case metric: RecordMetricMessage =>
                recordMetric(metric)
                MessageResult.Handled
        }
    }

    private func recordMetric(metric: RecordMetricMessage): Unit {
        match (metric.type) {
            case counter: CounterMetric =>
                metricsRegistry.getCounter(counter.name).increment(counter.value)
            case gauge: GaugeMetric =>
                metricsRegistry.getGauge(gauge.name).set(gauge.value)
            case histogram: HistogramMetric =>
                metricsRegistry.getHistogram(histogram.name).observe(histogram.value)
        }
    }
}
```

### 2. Grafana Dashboard

```json
{
  "dashboard": {
    "title": "CActor System Monitoring",
    "panels": [
      {
        "title": "Message Processing Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(cactor_messages_processed_total[5m])",
            "legendFormat": "Messages/sec"
          }
        ]
      },
      {
        "title": "Active Actors",
        "type": "singlestat",
        "targets": [
          {
            "expr": "cactor_active_actors",
            "legendFormat": "Active Actors"
          }
        ]
      },
      {
        "title": "Message Processing Latency",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, cactor_message_processing_duration_seconds)",
            "legendFormat": "P95 Latency"
          },
          {
            "expr": "histogram_quantile(0.99, cactor_message_processing_duration_seconds)",
            "legendFormat": "P99 Latency"
          }
        ]
      },
      {
        "title": "System Resources",
        "type": "graph",
        "targets": [
          {
            "expr": "cactor_cpu_usage_percent",
            "legendFormat": "CPU Usage %"
          },
          {
            "expr": "cactor_memory_usage_bytes / 1024 / 1024",
            "legendFormat": "Memory Usage MB"
          }
        ]
      }
    ]
  }
}
```

## Alerting System

### 1. Alert Rules

```cangjie
// Alert rule definition
public struct AlertRule {
    public let name: String
    public let condition: AlertCondition
    public let severity: AlertSeverity
    public let cooldown: Duration
    public let actions: Array<AlertAction>
}

public enum AlertSeverity {
    | Info
    | Warning
    | Critical
}

class AlertManager <: Actor {
    private let alertRules: ArrayList<AlertRule> = ArrayList()
    private let activeAlerts: HashMap<String, Alert> = HashMap()
    private let alertHistory: CircularBuffer<Alert> = CircularBuffer(1000)

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case metric: MetricUpdateMessage =>
                evaluateAlertRules(metric)
                MessageResult.Handled

            case query: AlertQueryMessage =>
                let alerts = getAlerts(query.criteria)
                context.sender.tell(AlertQueryResponse(alerts))
                MessageResult.Handled
        }
    }

    private func evaluateAlertRules(metric: MetricUpdateMessage): Unit {
        for (rule in alertRules) {
            if (rule.condition.evaluate(metric)) {
                triggerAlert(rule, metric)
            }
        }
    }

    private func triggerAlert(rule: AlertRule, metric: MetricUpdateMessage): Unit {
        let alertId = "${rule.name}-${System.currentTimeMillis()}"
        let alert = Alert(
            id: alertId,
            rule: rule,
            triggeredBy: metric,
            timestamp: DateTime.now(),
            severity: rule.severity
        )

        activeAlerts.put(alertId, alert)
        alertHistory.add(alert)

        // Execute alert actions
        for (action in rule.actions) {
            action.execute(alert)
        }
    }
}
```

### 2. Alert Notifications

```cangjie
// Alert notification implementation
public interface AlertAction {
    public func execute(alert: Alert): Unit
}

class EmailAlertAction <: AlertAction {
    private let emailService: EmailService
    private let recipients: Array<String>

    public func execute(alert: Alert): Unit {
        let subject = "CActor Alert: ${alert.rule.name}"
        let body = formatAlertEmail(alert)

        for (recipient in recipients) {
            emailService.sendEmail(recipient, subject, body)
        }
    }

    private func formatAlertEmail(alert: Alert): String {
        return """
        Alert: ${alert.rule.name}
        Severity: ${alert.severity}
        Time: ${alert.timestamp}

        Details:
        ${alert.triggeredBy.toString()}

        Dashboard: http://grafana.example.com/dashboard/cactor
        """
    }
}

class SlackAlertAction <: AlertAction {
    private let slackClient: SlackClient
    private let channel: String

    public func execute(alert: Alert): Unit {
        let message = SlackMessage(
            channel: channel,
            text: formatSlackMessage(alert),
            color: getColorForSeverity(alert.severity)
        )

        slackClient.sendMessage(message)
    }
}
```

## Monitoring Configuration

### 1. Monitoring Configuration File

```hocon
# config/monitoring.conf
cactor {
  monitoring {
    enabled = true

    metrics {
      collection-interval = "10s"
      retention-period = "7d"

      exporters {
        prometheus {
          enabled = true
          port = 8080
          path = "/metrics"
        }

        influxdb {
          enabled = false
          url = "http://localhost:8086"
          database = "cactor"
        }
      }
    }

    logging {
      level = "INFO"
      structured = true

      appenders {
        console {
          enabled = true
          pattern = "%d{ISO8601} [%level] %logger{36} - %msg%n"
        }

        file {
          enabled = true
          file = "logs/cactor.log"
          max-size = "100MB"
          max-files = 10
        }

        elasticsearch {
          enabled = false
          hosts = ["localhost:9200"]
          index = "cactor-logs"
        }
      }
    }

    alerts {
      enabled = true

      rules = [
        {
          name = "high-error-rate"
          condition = "error_rate > 0.05"
          severity = "warning"
          cooldown = "5m"
          actions = ["email", "slack"]
        },
        {
          name = "high-latency"
          condition = "p99_latency > 1000ms"
          severity = "critical"
          cooldown = "2m"
          actions = ["email", "slack", "pagerduty"]
        }
      ]
    }
  }
}
```

---

**CActor v1.2.0 - Comprehensive Monitoring, Real-Time Insight!**

_Last updated: July 3, 2025_