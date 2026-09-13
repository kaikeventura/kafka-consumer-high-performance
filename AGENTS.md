# AGENTS.md - AI Agent Instructions

## Project Overview

High-performance Kafka message processing pipeline: Go Load Generator → Kafka (KRaft) → Java Spring Boot (6 replicas) → SQS via Floci.

## Quick Start

```bash
docker compose up -d
```

## Build & Run

```bash
# Build app
docker compose build app

# Run load test
cd load_generator && go run main.go 1000000

# Run monitor
cd monitor && go run main.go
```

## Key Files

- `docker-compose.yml` - All infrastructure
- `app/src/main/resources/application.yml` - Spring config, metrics, Kafka settings
- `app/src/main/java/com/highperformance/kafka/listener/MessageListener.java` - Kafka consumer
- `load_generator/main.go` - Go load generator
- `monitor/main.go` - Go dashboard
- `README.md` - Documentation with benchmarks
- `AGENTS.md` - This file

## Architecture

```
Load Generator (Go) → Kafka (KRaft) → Spring Boot App (6 replicas) → SQS via Floci
```

- Kafka runs in KRaft mode (no Zookeeper) with 6 partitions
- Each Spring Boot container has `concurrency=1`, consuming 1 partition
- Load generator uses single shared kafka.Writer with sync mode
- Monitor polls Actuator metrics every 2s

## Important Details

- **Floci SQS**: Emulates AWS SQS at `http://localhost:4566` (in Docker: `http://floci:4566`)
- **Queue URL format**: `http://floci:4566/000000000000/<queue-name>`
- **Kafka dual listeners**: INTERNAL (`kafka:29092`) for containers, EXTERNAL (`localhost:9092`) for host
- **Topic must be created manually** if `kafka-setup` service fails:
  ```bash
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic input-topic --partitions 6 --replication-factor 1
  ```
- **Metric names**:
  - `spring.kafka.listener` - Timer (COUNT, TOTAL_TIME, MAX)
  - `spring.kafka.listener.percentile` - P50/P90/P99 (tag: `phi:0.5`, `phi:0.9`, `phi:0.99`)
  - `kafka.consumer.fetch.manager.records.lag` - Lag per partition
- **Percentile config** in `application.yml` uses map format:
  ```yaml
  management.metrics.distribution.percentiles:
    spring.kafka.listener: 0.5,0.9,0.99
  management.metrics.distribution.percentiles-histogram:
    spring.kafka.listener: true
  ```

## Current Configuration

### Kafka Consumer
```yaml
spring:
  kafka:
    bootstrap-servers: kafka:29092
    topic:
      input: input-topic
    consumer:
      group-id: high-perf-consumer-group
      auto-offset-reset: earliest
      max.poll.records: 500
      fetch.min.bytes: 1
      fetch.max.wait.ms: 100
    listener:
      concurrency: 1
```

### Infrastructure
- **Kafka**: 6 partitions, KRaft mode
- **Containers**: 6 Spring Boot replicas
- **Load Generator**: 48 workers, synchronous mode, batch size 1000
- **Resource Limits**: 0.5 CPU, 1GB memory per container

### Processing Delay (Optional)
```bash
# Simulate slow processing to test lag
PROCESSING_DELAY_MS=50 docker compose up -d --build app

# Default (no delay)
PROCESSING_DELAY_MS=0 docker compose up -d --build app
```

## Benchmarks

Benchmarks are documented in `README.md` under the `## Benchmarks` section. Each benchmark includes:
- Configuration parameters
- Monitor output (with CPU/Memory stats and SQS verification)
- Metrics table (throughput, latency, lag)

To add a new benchmark, copy the template in README.md and fill in the results.

## Monitor Features

The monitor displays:
- **Run Status**: Start/end time, duration, total messages, avg/peak rate
- **SQS Queue**: Message count in SQS (verifies no message loss)
- **Summary**: Active containers, processed messages, Kafka lag
- **Per Container**: Port, messages, lag (max), P50/P90/P99 latency, CPU/Memory (min/med/max)

## Common Commands

```bash
# Check containers
docker ps

# Check consumer group lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group high-perf-consumer-group --describe

# List topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check metrics
curl http://localhost:8080/actuator/metrics | python3 -m json.tool

# Check lag metric
curl -s http://localhost:8080/actuator/metrics/kafka.consumer.fetch.manager.records.lag | python3 -m json.tool

# Check percentile P50
curl -s "http://localhost:8080/actuator/metrics/spring.kafka.listener.percentile?tag=phi:0.5" | python3 -m json.tool

# Check SQS message count
aws sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/output-queue --attribute-names ApproximateNumberOfMessages --endpoint-url http://localhost:4566

# Check Docker stats
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `input-topic` | Kafka topic name |
| `NUM_WORKERS` | `48` | Number of goroutines in load generator |
| `BATCH_SIZE` | `1000` | Kafka writer batch size |
| `BASE_PORT` | `8080` | First Spring Boot port |
| `NUM_CONTAINERS` | `6` | Number of containers to monitor |
| `INTERVAL_SEC` | `2` | Monitor polling interval |
| `PROCESSING_DELAY_MS` | `0` | Delay per message (ms) for lag testing |

## Troubleshooting

1. **Topic not created**: Run the kafka-topics.sh command manually
2. **Kafka connection refused**: Check dual listeners in docker-compose.yml
3. **Metrics showing 0**: Verify `percentiles-histogram` is enabled in application.yml
4. **Load generator fails**: Ensure Kafka is healthy: `docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092`
5. **Monitor shows 0 latency**: Check if `management.metrics.distribution.publish-percentiles: true` is set
6. **SQS queue not found**: Create it: `aws sqs create-queue --queue-name output-queue --endpoint-url http://localhost:4566`

## File Structure Reference

```
kafka-consumer-high-performance/
├── docker-compose.yml
├── README.md
├── AGENTS.md
├── load_generator/
│   ├── main.go
│   └── go.mod
├── monitor/
│   ├── main.go
│   └── go.mod
└── app/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/
        ├── java/com/highperformance/kafka/
        │   ├── KafkaConsumerApplication.java
        │   ├── config/
        │   │   └── SqsConfig.java
        │   ├── listener/
        │   │   └── MessageListener.java
        │   └── service/
        │       └── SqsProducerService.java
        └── resources/
            └── application.yml
```

## Tech Stack

- **Kafka**: Apache Kafka 3.7.1 (KRaft mode)
- **Spring Boot**: 3.3.4
- **Java**: 21 (Eclipse Temurin Alpine)
- **Go**: 1.21+ (kafka-go)
- **SQS Emulator**: Floci (LocalStack-compatible)
- **Containerization**: Docker Compose
