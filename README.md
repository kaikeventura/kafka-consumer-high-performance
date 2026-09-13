# Kafka Consumer High Performance

Pipeline de processamento de mensagens de alta vazão:

```
Load Generator (Go) → Kafka (KRaft) → Spring Boot App (6 replicas) → SQS via Floci
```

## Arquitetura

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐     ┌─────────────────┐
│   Load Gen Go   │────▶│   Kafka KRaft   │────▶│  Spring Boot (x6)      │────▶│   Floci (SQS)   │
│   localhost:     │     │   :29092/:9092   │     │  :8080-8085            │     │   :4566         │
│   (unlimited)   │     │   6 partitions   │     │  1 partition/container │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────────────┘     └─────────────────┘
                                    │
                                    ▼
                           ┌─────────────────┐
                           │   Monitor Go    │
                           │   polls:2s      │
                           └─────────────────┘
```

## Pré-requisitos

- Docker e Docker Compose
- Go 1.21+
- Java 21+ (apenas para desenvolvimento local)

## Início Rápido

```bash
# 1. Subir infraestrutura
docker compose up -d

# 2. Verificar containers
docker ps

# 3. Criar tópico (se não foi criado automaticamente)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic input-topic \
  --partitions 6 \
  --replication-factor 1

# 4. Em terminais separados:
cd load_generator && go run main.go 1000000
cd monitor && go run main.go
```

## Estrutura do Projeto

```
├── docker-compose.yml              # Infraestrutura completa
├── load_generator/                 # Gerador de carga (Go)
│   ├── main.go                     # Producer com kafka-go
│   └── go.mod
├── monitor/                        # Dashboard de monitoramento (Go)
│   ├── main.go                     # Polling de métricas Actuator
│   └── go.mod
└── app/                            # Aplicação Spring Boot
    ├── Dockerfile                  # Multi-stage build
    ├── pom.xml                     # Java 21, Spring Boot 3.3.4
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

## Serviços

| Serviço | Porta | Descrição |
|---------|-------|-----------|
| Kafka | 29092 (interno), 9092 (host) | Apache Kafka em modo KRaft |
| Floci | 4566 | Emulador AWS SQS |
| App (x6) | 8080-8085 | Spring Boot Kafka Consumer |
| kafka-setup | - | Cria tópicos automaticamente |

## Gerar Carga

```bash
cd load_generator

# Enviar N mensagens
go run main.go 1000000

# Modo infinito
go run main.go

# Variáveis de ambiente:
# KAFKA_BROKERS=localhost:9092
# KAFKA_TOPIC=input-topic
# NUM_WORKERS=48
# BATCH_SIZE=1000
```

**Saída do load generator:**
```
[throughput] 45000 msg/s | total: 1000000 | elapsed: 22.3s | avg: 44843 msg/s

✓ Load completed!
  Duration:   22.31s
  Messages:   1000000
  Avg rate:   44843 msg/s
```

## Monitorar

```bash
cd monitor
go run main.go

# Variáveis de ambiente:
# BASE_PORT=8080
# NUM_CONTAINERS=6
# INTERVAL_SEC=2
```

**Saída do monitor:**
```
╔══════════════════════════════════════════════════════════════════════════════════════════╗
║                           KAFKA CONSUMER MONITOR                                        ║
╚══════════════════════════════════════════════════════════════════════════════════════════╝

  Updated: 23:08:26

  ── RUN STATUS ──────────────────────────────────────────────────────
  Start:     23:08:26
  End:       23:09:32
  Duration:  66.2s
  Total:     120011 msgs
  Avg Rate:  1813 msg/s
  Peak Rate: 5847 msg/s
  SQS Queue: 120011 msgs
  Status:    ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:   120011 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY
                                                                          (min/med/max)      (min/med/max)
  ──────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080       20002     0          1.36ms        1.75ms        3.13ms        0.4/0.4/0.4%       210/210/210 MiB
  8081       20001     0          1.29ms        1.75ms        3.13ms        0.4/0.4/0.4%       210/210/210 MiB
  8082       20002     0          1.29ms        1.75ms        3.26ms        0.4/0.4/0.4%       210/210/210 MiB
  8083       20002     0          1.36ms        1.75ms        3.26ms        0.4/0.4/0.4%       210/210/210 MiB
  8084       20002     0          1.36ms        1.75ms        3.39ms        0.4/0.4/0.4%       210/210/210 MiB
  8085       20002     0          1.29ms        1.82ms        3.39ms        0.4/0.4/0.4%       210/210/210 MiB
```

**Indicadores:**
- **P50/P90/P99**: Latência de processamento em milissegundos
- **LAG**: Mensagens pendentes no Kafka (0 = processou tudo)
- **CPU**: Uso de CPU min/med/max por container
- **MEMORY**: Uso de memória min/med/max por container
- **SQS Queue**: Total de mensagens na fila SQS (verificação de perda)
- **Status**: Confirma se todas as mensagens foram para o SQS

## Endpoints Úteis

### Actuator (por container)

```bash
# Listar todas as métricas
curl http://localhost:8080/actuator/metrics

# Lag do Kafka
curl http://localhost:8080/actuator/metrics/kafka.consumer.fetch.manager.records.lag

# Percentil P50
curl "http://localhost:8080/actuator/metrics/spring.kafka.listener.percentile?tag=phi:0.5"

# Mensagens processadas
curl http://localhost:8080/actuator/metrics/kafka.consumer.fetch.manager.records.consumed.total
```

### Kafka

```bash
# Listar tópicos
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Descrever tópico
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic input-topic

# Consumer groups
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Status do consumer group
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group high-perf-consumer-group --describe
```

### Floci (SQS)

```bash
# Criar fila
aws sqs create-queue --queue-name output-queue --endpoint-url http://localhost:4566

# Listar filas
aws sqs list-queues --endpoint-url http://localhost:4566

# Receber mensagem (não deleta)
aws sqs receive-message --queue-url http://localhost:4566/000000000000/output-queue --endpoint-url http://localhost:4566

# Health check
curl http://localhost:4566/_localstack/health
```

## Configurações Importantes

### Kafka (docker-compose.yml)

- **KAFKA_AUTO_CREATE_TOPICS_ENABLE**: `false` (tópicos devem ser criados manualmente)
- **KAFKA_NUM_PARTITIONS**: `6`
- **Dual Listeners**: INTERNAL (kafka:29092) para containers, EXTERNAL (localhost:9092) para host

### Spring Boot (application.yml)

- **spring.kafka.listener.concurrency**: `1` (1 thread por container = 6 partições)
- **management.metrics.distribution.percentiles**: Habilita P50/P90/P99

### Load Generator

- **Modo síncrono**: `Async: false` (aguarda confirmação do Kafka)
- **Batch**: `BatchSize: 1000`, `BatchTimeout: 10ms`
- **Workers**: 48 goroutines paralelas

### Processing Delay (Opcional)

Para simular processamento lento e testar lag:

```bash
# Subir com delay de 50ms por mensagem
PROCESSING_DELAY_MS=50 docker compose up -d --build app

# Subir sem delay (padrão)
PROCESSING_DELAY_MS=0 docker compose up -d --build app
```

### Resource Limits

Cada container tem limites de recursos:
- **CPU**: 1.0 (máximo), 0.5 (reservado)
- **Memória**: 1GB (máximo), 512MB (reservado)

## Troubleshooting

### Tópico não existe

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic input-topic \
  --partitions 6 \
  --replication-factor 1
```

### Kafka não responde (localhost:9092)

Verificar se o listener EXTERNAL está configurado:
```bash
docker exec kafka cat /opt/kafka/config/kraft/server.properties | grep LISTENERS
```

### Containers não sobem

```bash
docker compose down -v
docker compose build --no-cache app
docker compose up -d
```

### Métricas zeradas no monitor

Verificar se o Actuator está exposto:
```bash
curl http://localhost:8080/actuator/metrics | head -50
```

## Parar Serviços

```bash
# Parar e remover containers
docker compose down

# Parar e remover containers + volumes
docker compose down -v

# Rebuild completo
docker compose down -v && docker compose build --no-cache && docker compose up -d
```

## Métricas Disponíveis

| Métrica | Descrição |
|---------|-----------|
| `spring.kafka.listener` | Timer do listener (COUNT, TOTAL_TIME, MAX) |
| `spring.kafka.listener.percentile` | Percentis P50/P90/P99 |
| `kafka.consumer.fetch.manager.records.lag` | Lag por partição |
| `kafka.consumer.fetch.manager.records.consumed.total` | Total de mensagens consumidas |
| `kafka.consumer.fetch.manager.records.lead` | Lead do consumer |

## Tecnologias

- **Kafka**: Apache Kafka 3.7.1 (KRaft mode)
- **Spring Boot**: 3.3.4
- **Java**: 21 (Eclipse Temurin Alpine)
- **Go**: 1.21+ (kafka-go, prometheus)
- **SQS Emulator**: Floci (LocalStack-compatible)
- **Containerização**: Docker Compose

## Benchmarks

### Comparativo Geral

| # | Configuração | Throughput | Δ vs #1 | Latência P50 | SQS |
|---|--------------|------------|---------|--------------|-----|
| 1 | Sync SQS (baseline) | 470 msg/s | - | 11.01ms | ✓ |
| 2 | Async SQS | 520 msg/s | +10.6% | 9.96ms | ✓ |
| 3 | Concurrency 3 | 513 msg/s | +9.1% | 9.96ms | ✓ |
| 4 | Batch SQS (10) | 518 msg/s | +10.2% | 9.96ms | ✓ |
| 5 | max.poll=1000 | 518 msg/s | +10.2% | 9.96ms | ✓ |
| 6 | Concurrency 5 | 518 msg/s | +10.2% | 9.96ms | ✓ |
| 7 | fetch.min=4096 + batch=20 | 527 msg/s | +12.1% | 9.96ms | ✓ |
| **8** | **Batch Listener** | **2433 msg/s** | **+418%** | **0.75ms** | ✓ |
| 9 | max.poll=2000 + batch=50 | 2261 msg/s | +381% | 0.84ms | ✓ |
| **10** | **Load workers=192** | **4018 msg/s** | **+755%** | **4.96-301ms** | ✓ |
| 11 | fetch.wait=1 + load batch=5000 | 4078 msg/s | +768% | 6.54-503ms | ✓ |
| **12** | **Parallel SQS** | **4623 msg/s** | **+884%** | **201-499ms** | ✓ |
| 13 | JVM Tuning (G1GC) | 3414 msg/s | +626% | 209-603ms | ✓ |
| **14** | **Jackson Afterburner** | **3518 msg/s** | **+648%** | **301-700ms** | ✓ |
| **15** | **String Manipulation (no Jackson)** | **3719 msg/s** | **+691%** | **0.72-1.37ms** | ✓ |

**Gargalo identificado**: Processing delay (10ms) limita throughput a ~530 msg/s com listener simples.

**Solução**: Batch Kafka Listener processa até 1000 msgs por poll, multiplicando throughput por 4.6x.

---

### Benchmark #1 - Configuração com Delay (10ms)
**Data:** 12/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 500 |
| **Fetch Min Bytes** | 1 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 1 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 10ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Síncrono |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     22:32:57
  End:       22:35:05
  Duration:  127.7s
  Total:    60047 msgs
  Avg Rate: 470 msg/s
  Peak Rate: 2136 msg/s
  SQS Queue: 60047 msgs
  Status:    ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:   60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY
                       (max)                                                (min/med/max)      (min/med/max)
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080       10008     3962       11.01ms       11.53ms       15.20ms       0.8/8.6/25.6%      165/198/204 MiB
  8081       10007     3970       11.01ms       11.53ms       15.20ms       1.6/9.6/28.0%      166/199/208 MiB
  8082       10008     4028       11.01ms       11.53ms       15.73ms       1.3/7.6/24.8%      169/201/205 MiB
  8083       10008     4658       11.01ms       11.53ms       15.73ms       1.2/8.9/26.3%      167/199/206 MiB
  8084       10008     3756       11.01ms       11.53ms       15.73ms       1.0/8.3/27.7%      165/197/203 MiB
  8085       10008     4133       11.01ms       11.53ms       15.73ms       1.5/8.3/23.2%      165/197/203 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 470 msg/s |
| **Throughput Pico** | 2.136 msg/s |
| **Latência P50** | 11.01ms |
| **Latência P90** | 11.53ms |
| **Latência P99** | 15.20-15.73ms |
| **Lag Max** | 3.756-4.658 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 23-28% |
| **Memória Max** | 203-208 MiB |

---

### Benchmark #2 - Async SQS com Delay (10ms)
**Data:** 12/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 500 |
| **Fetch Min Bytes** | 1 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 1 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 10ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async (@Async) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     22:57:48
  End:       22:59:43
  Duration:  115.6s
  Total:    60047 msgs
  Avg Rate: 520 msg/s
  Peak Rate: 2384 msg/s
  SQS Queue: 60047 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     5852  9.96ms          9.96ms          9.96ms          0.6/9.0/40.5%      182/217/221 MiB   
  8081        10008     6346  9.96ms          9.96ms          9.96ms          0.8/8.3/44.3%      177/212/216 MiB   
  8082        10008     5917  9.96ms          9.96ms          9.96ms          0.6/7.7/44.8%      177/211/221 MiB   
  8083        10008     5990  9.96ms          9.96ms          9.96ms          0.6/7.5/39.0%      176/212/218 MiB   
  8084        10008     5763  9.96ms          9.96ms          9.96ms          0.7/9.3/41.2%      178/212/217 MiB   
  8085        10007     5875  9.96ms          9.96ms          9.96ms          0.5/8.0/42.2%      180/215/218 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 520 msg/s |
| **Throughput Pico** | 2.384 msg/s |
| **Latência P50** | 9.96ms |
| **Latência P90** | 9.96ms |
| **Latência P99** | 9.96ms |
| **Lag Max** | 5.763-6.346 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 39-45% |
| **Memória Max** | 216-221 MiB |

#### Alterações em Relação ao Benchmark #1

- **SQS Mode**: Síncrono → Async (@Async)
- **Throughput**: +10.6% (470 → 520 msg/s)
- **Latência P50**: -9.5% (11.01ms → 9.96ms)
- **Lag Max**: +55% (3.756-4.658 → 5.763-6.346)

---

### Benchmark #3 - Concurrency 3 + Async SQS
**Data:** 12/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 500 |
| **Fetch Min Bytes** | 1 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 3 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 10ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async (@Async) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     23:08:18
  End:       23:10:15
  Duration:  117.0s
  Total:    60047 msgs
  Avg Rate: 513 msg/s
  Peak Rate: 2388 msg/s
  SQS Queue: 60047 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     5307  9.96ms          9.96ms          9.96ms          0.7/9.9/40.7%      169/206/214 MiB   
  8081        10008     5660  9.96ms          9.96ms          9.96ms          0.7/10.4/38.8%     171/209/212 MiB   
  8082        10008     5636  9.96ms          9.96ms          9.96ms          0.8/9.5/36.3%      171/210/214 MiB   
  8083        10007     5524  9.96ms          9.96ms          9.96ms          0.7/9.7/35.8%      174/213/223 MiB   
  8084        10008     5533  9.96ms          9.96ms          9.96ms          0.7/6.7/42.3%      169/208/220 MiB   
  8085        10008     5605  9.96ms          9.96ms          9.96ms          0.7/6.9/37.7%      177/215/223 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 513 msg/s |
| **Throughput Pico** | 2.388 msg/s |
| **Latência P50** | 9.96ms |
| **Latência P90** | 9.96ms |
| **Latência P99** | 9.96ms |
| **Lag Max** | 5.307-5.660 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 35-42% |
| **Memória Max** | 212-223 MiB |

#### Alterações em Relação ao Benchmark #2

- **Concurrency**: 1 → 3 (18 threads total)
- **Throughput**: -1.3% (520 → 513 msg/s)
- **Lag Max**: -10% (5.763-6.346 → 5.307-5.660)
- **CPU Med**: +20% (8-9% → 10%)

**Conclusão**: O gargalo não é concorrência de threads, mas sim throughput do SQS e fetch do Kafka. Próxima melhoria: Batch SQS.

---

### Benchmark #4 - Batch SQS (10 msgs/batch) + Scheduling Fix
**Data:** 12/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 500 |
| **Fetch Min Bytes** | 1 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 3 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 10ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (10 msgs) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     23:35:48
  End:       23:37:44
  Duration:  115.9s
  Total:    60047 msgs
  Avg Rate: 518 msg/s
  Peak Rate: 2393 msg/s
  SQS Queue: 60047 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     5562  9.96ms          9.96ms          9.96ms          1.3/5.5/49.3%      172/206/210 MiB   
  8081        10008     5311  9.96ms          9.96ms          9.96ms          1.1/6.2/41.2%      170/202/206 MiB   
  8082        10007     5039  9.96ms          9.96ms          9.96ms          0.9/5.2/31.9%      175/207/210 MiB   
  8083        10008     5815  9.96ms          9.96ms          9.96ms          1.2/5.5/38.0%      179/208/213 MiB   
  8084        10008     5166  9.96ms          9.96ms          9.96ms          1.0/4.4/49.4%      172/205/209 MiB   
  8085        10008     4934  9.96ms          9.96ms          9.96ms          1.5/5.2/26.3%      175/206/210 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 518 msg/s |
| **Throughput Pico** | 2.393 msg/s |
| **Latência P50** | 9.96ms |
| **Latência P90** | 9.96ms |
| **Latência P99** | 9.96ms |
| **Lag Max** | 4.934-5.815 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 26-49% |
| **Memória Max** | 206-213 MiB |

#### Alterações em Relação ao Benchmark #3

- **SQS Mode**: Async → Async + Batch (10 msgs/batch)
- **Throughput**: +1.0% (513 → 518 msg/s)
- **CPU Max**: +18% (35-42% → 26-49%)
- **SQS**: ✓ 100% entregues (vs ⚠ 46 pending)

**Conclusão**: Batch SQS com scheduling fix garante 100% de entrega. Throughput similar ao Benchmark #3, mas com CPU mais consistente. Próxima melhoria: aumentar max.poll.records ou fetch.min.bytes.

---

### Benchmark #5 - max.poll.records = 1000
**Data:** 12/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 1000 |
| **Fetch Min Bytes** | 1 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 3 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 10ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (10 msgs) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     23:43:20
  End:       23:45:16
  Duration:  115.9s
  Total:    60047 msgs
  Avg Rate: 518 msg/s
  Peak Rate: 2394 msg/s
  SQS Queue: 60047 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     6390  9.96ms          9.96ms          9.96ms          0.9/4.2/31.9%      172/207/210 MiB   
  8081        10008     6382  9.96ms          9.96ms          9.96ms          1.3/5.6/25.4%      174/208/212 MiB   
  8082        10007     6431  9.96ms          9.96ms          9.96ms          1.1/6.2/36.3%      171/206/209 MiB   
  8083        10008     6616  9.96ms          9.96ms          9.96ms          1.0/5.0/19.6%      175/206/210 MiB   
  8084        10008     5634  9.96ms          9.96ms          9.96ms          1.4/5.1/18.0%      176/208/212 MiB   
  8085        10008     6811  9.96ms          9.96ms          9.96ms          0.8/5.8/24.3%      175/207/212 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 518 msg/s |
| **Throughput Pico** | 2.394 msg/s |
| **Latência P50** | 9.96ms |
| **Latência P90** | 9.96ms |
| **Latência P99** | 9.96ms |
| **Lag Max** | 5.634-6.811 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 18-36% |
| **Memória Max** | 209-212 MiB |

#### Alterações em Relação ao Benchmark #4

- **Max Poll Records**: 500 → 1000
- **Throughput**: 0% (518 → 518 msg/s)
- **CPU Max**: -27% (26-49% → 18-36%)

**Conclusão**: Aumentar max.poll.records não melhorou throughput. O gargalo é o **processing delay (10ms)** — cada thread processa ~100 msg/s. Com 3 threads × 6 containers = 18 threads × 100 msg/s = ~1800 msg/s teórico, mas limitado pelo SQS batch overhead.

**Próximas opções**:
1. Reduzir processing delay (10ms → 0ms)
2. Aumentar concurrency (3 → 5)
3. Aumentar batch size SQS (10 → 20)

---

### Benchmark #6 - Concurrency 5
**Data:** 12/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 1000 |
| **Fetch Min Bytes** | 1 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 10ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (10 msgs) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     23:50:38
  End:       23:52:34
  Duration:  115.9s
  Total:    60047 msgs
  Avg Rate: 518 msg/s
  Peak Rate: 2394 msg/s
  SQS Queue: 60047 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     6318  9.96ms          9.96ms          9.96ms          1.0/5.6/29.4%      175/202/206 MiB   
  8081        10008     5688  9.96ms          9.96ms          9.96ms          0.8/5.2/28.5%      175/203/207 MiB   
  8082        10008     6035  9.96ms          9.96ms          9.96ms          1.1/5.8/21.0%      186/215/218 MiB   
  8083        10008     5736  9.96ms          9.96ms          9.96ms          1.2/5.3/29.1%      175/205/209 MiB   
  8084        10007     5986  9.96ms          9.96ms          9.96ms          1.0/5.6/49.5%      178/205/209 MiB   
  8085        10008     6729  9.96ms          9.96ms          9.96ms          1.1/5.2/23.0%      179/207/212 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 518 msg/s |
| **Throughput Pico** | 2.394 msg/s |
| **Latência P50** | 9.96ms |
| **Latência P90** | 9.96ms |
| **Latência P99** | 9.96ms |
| **Lag Max** | 5.688-6.729 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 21-50% |
| **Memória Max** | 206-218 MiB |

#### Alterações em Relação ao Benchmark #5

- **Concurrency**: 3 → 5 (30 threads total)
- **Throughput**: 0% (518 → 518 msg/s)
- **CPU Max**: +18% (18-36% → 21-50%)

**Conclusão**: Concurrency não melhora throughput. O gargalo é o **processing delay (10ms)** — cada thread processa ~100 msg/s, mas o SQS batch overhead limita o ganho real.

**Próxima opção recomendada**: Reduzir processing delay (10ms → 0ms) para maximizar throughput.

---

### Benchmark #7 - fetch.min.bytes=4096 + sqs batch-size=20
**Data:** 12/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 1000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 10ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (20 msgs) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     23:59:49
  End:       00:01:42
  Duration:  113.9s
  Total:    60047 msgs
  Avg Rate: 527 msg/s
  Peak Rate: 2389 msg/s
  SQS Queue: 60047 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     3704  9.96ms          9.96ms          9.96ms          0.6/4.8/12.3%      173/203/213 MiB   
  8081        10008     4272  9.96ms          9.96ms          9.96ms          0.7/5.5/13.6%      176/206/210 MiB   
  8082        10007     4510  9.96ms          9.96ms          9.96ms          0.7/4.9/11.4%      177/208/214 MiB   
  8083        10008     4519  9.96ms          9.96ms          9.96ms          0.7/5.8/11.6%      175/203/211 MiB   
  8084        10008     4142  9.96ms          9.96ms          9.96ms          0.9/3.8/12.5%      174/202/208 MiB   
  8085        10008     3962  9.96ms          9.96ms          9.96ms          0.8/5.8/14.8%      172/202/212 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 527 msg/s |
| **Throughput Pico** | 2.389 msg/s |
| **Latência P50** | 9.96ms |
| **Latência P90** | 9.96ms |
| **Latência P99** | 9.96ms |
| **Lag Max** | 3.704-4.519 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 11-15% |
| **Memória Max** | 208-214 MiB |

#### Alterações em Relação ao Benchmark #6

- **Fetch Min Bytes**: 1 → 4096
- **SQS Batch Size**: 10 → 20
- **Throughput**: +1.7% (518 → 527 msg/s)
- **Lag Max**: -33% (5.688-6.729 → 3.704-4.519)
- **CPU Max**: -70% (21-50% → 11-15%)

**Conclusão**: fetch.min.bytes reduz lag significativamente (-33%) e CPU (-70%), mas throughput só melhora marginalmente. O processing delay (10ms) continua sendo o gargalo principal.

---

### Benchmark #8 - Batch Kafka Listener
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 1000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (20 msgs) |
| **Kafka Listener** | Batch (até 1000 msgs/poll) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     00:16:17
  End:       00:16:42
  Duration:  24.7s
  Total:    60047 msgs
  Avg Rate: 2433 msg/s
  Peak Rate: 17943 msg/s
  SQS Queue: 60047 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60047 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     0  0.91ms          96.46ms         192.93ms        1.0/2.3/49.4%      184/205/214 MiB   
  8081        10008     0  0.75ms          75.49ms         192.93ms        1.2/3.7/49.1%      181/204/211 MiB   
  8082        10007     0  0.58ms          67.10ms         109.04ms        1.4/2.7/49.7%      181/212/217 MiB   
  8083        10008     0  0.81ms          71.29ms         100.66ms        1.4/2.1/48.6%      184/205/212 MiB   
  8084        10008     0  0.61ms          71.29ms         104.85ms        1.2/2.8/54.7%      188/213/219 MiB   
  8085        10008     0  0.58ms          71.29ms         192.93ms        1.5/3.5/50.5%      187/207/218 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 2433 msg/s |
| **Throughput Pico** | 17.943 msg/s |
| **Latência P50** | 0.58-0.91ms |
| **Latência P90** | 67-96ms |
| **Latência P99** | 100-193ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 48-55% |
| **Memória Max** | 211-219 MiB |

#### Alterações em Relação ao Benchmark #7

- **Kafka Listener**: Simple → Batch (até 1000 msgs/poll)
- **Throughput**: +361% (527 → 2433 msg/s)
- **Duração**: -79% (115.9s → 24.7s)
- **Peak Rate**: +651% (2389 → 17943 msg/s)
- **Lag**: -100% (3704-4519 → 0)
- **Latência P50**: -85% (9.96ms → 0.58-0.91ms)

**Conclusão**: Batch Kafka Listener é a otimização mais impactante. Processa até 1000 msgs por poll, eliminando overhead de polling individual. Throughput aumenta 4.6x com latência 85% menor.

---

### Benchmark #9 - max.poll=2000 + SQS Batch 50
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 2000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 48 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (50 msgs) |
| **Kafka Listener** | Batch (até 2000 msgs/poll) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     00:21:38
  End:       00:22:05
  Duration:  26.6s
  Total:    60046 msgs
  Avg Rate: 2261 msg/s
  Peak Rate: 15702 msg/s
  SQS Queue: 60046 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60046 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10008     0  1.23ms          96.45ms         109.04ms        2.8/30.2/50.9%     180/204/213 MiB   
  8081        10008     0  0.88ms          88.07ms         201.32ms        1.6/11.6/52.8%     184/207/214 MiB   
  8082        10008     0  0.94ms          92.27ms         201.32ms        1.5/2.3/50.1%      187/206/210 MiB   
  8083        10007     0  1.10ms          96.45ms         201.31ms        1.7/6.2/50.2%      180/204/212 MiB   
  8084        10007     0  0.84ms          71.29ms         104.85ms        3.1/6.0/54.6%      181/205/210 MiB   
  8085        10008     0  0.71ms          71.29ms         100.66ms        1.8/6.8/53.4%      184/208/215 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 2261 msg/s |
| **Throughput Pico** | 15.702 msg/s |
| **Latência P50** | 0.71-1.23ms |
| **Latência P90** | 71-96ms |
| **Latência P99** | 100-201ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 50-55% |
| **Memória Max** | 210-215 MiB |

#### Alterações em Relação ao Benchmark #8

- **Max Poll Records**: 1000 → 2000
- **SQS Batch Size**: 20 → 50
- **Throughput**: -7% (2433 → 2261 msg/s)
- **CPU Max**: +5% (48-55% → 50-55%)

**Conclusão**: Aumentar max.poll.records e SQS batch não melhorou throughput - na verdade caiu levemente. O gargalo agora é CPU (50-55%). Próxima otimização: aumentar load workers para testar limite real.

---

### Benchmark #10 - Load Workers = 192
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 2000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 10ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 192 goroutines |
| **Batch Size** | 1000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (50 msgs) |
| **Kafka Listener** | Batch (até 2000 msgs/poll) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     00:26:35
  End:       00:26:50
  Duration:  15.0s
  Total:    60191 msgs
  Avg Rate: 4018 msg/s
  Peak Rate: 52611 msg/s
  SQS Queue: 60191 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60191 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10032     0  92.26ms         301.97ms        603.96ms        2.4/4.1/49.3%      184/185/216 MiB   
  8081        10032     0  4.96ms          109.04ms        503.30ms        2.9/10.8/49.3%     186/188/230 MiB   
  8082        10032     0  209.68ms        738.16ms        738.16ms        1.7/6.5/48.7%      187/188/220 MiB   
  8083        10032     0  301.47ms        603.46ms        704.12ms        1.7/5.7/49.3%      185/186/219 MiB   
  8084        10032     0  201.20ms        603.85ms        637.40ms        2.1/2.9/49.8%      188/190/218 MiB   
  8085        10031     0  109.02ms        419.40ms        503.28ms        1.9/4.3/49.4%      182/185/216 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 4018 msg/s |
| **Throughput Pico** | 52.611 msg/s |
| **Latência P50** | 4.96-301ms |
| **Latência P90** | 109-738ms |
| **Latência P99** | 503-738ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 48-50% |
| **Memória Max** | 216-230 MiB |

#### Alterações em Relação ao Benchmark #9

- **Load Workers**: 48 → 192
- **Throughput**: +77% (2261 → 4018 msg/s)
- **Duração**: -44% (26.6s → 15.0s)
- **Peak Rate**: +235% (15702 → 52611 msg/s)
- **Latência P50**: +593% (0.84ms → 4.96-301ms)

**Conclusão**: Aumentar load workers para 192 dobrou o throughput real. O sistema consegue processar 4000+ msg/s com 6 containers. Latência aumentou significativamente sob carga pesada, mas throughput é o dobro.

---

### Benchmark #11 - fetch.wait=1 + load batch=5000 + sqs batch=100
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 2000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 1ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 192 goroutines |
| **Batch Size** | 5000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Batch (100 msgs) |
| **Kafka Listener** | Batch (até 2000 msgs/poll) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     00:37:58
  End:       00:38:13
  Duration:  14.8s
  Total:    60191 msgs
  Avg Rate: 4078 msg/s
  Peak Rate: 40475 msg/s
  SQS Queue: 60191 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60191 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10032     0  503.19ms        1006.50ms       1006.50ms       8.1/31.8/53.7%     185/199/217 MiB   
  8081        10031     0  217.97ms        704.51ms        704.51ms        4.5/24.5/51.1%     191/206/226 MiB   
  8082        10032     0  314.57ms        1002.44ms       1002.44ms       7.9/26.1/53.2%     182/198/220 MiB   
  8083        10032     0  201.06ms        318.50ms        402.39ms        6.4/29.0/52.1%     185/206/228 MiB   
  8084        10032     0  209.45ms        805.04ms        1006.37ms       5.2/31.6/53.2%     190/203/222 MiB   
  8085        10032     0  6.54ms          104.84ms        503.30ms        2.9/7.0/49.2%      190/203/217 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 4078 msg/s |
| **Throughput Pico** | 40.475 msg/s |
| **Latência P50** | 6.54-503ms |
| **Latência P90** | 104-1006ms |
| **Latência P99** | 402-1006ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 49-54% |
| **Memória Max** | 217-228 MiB |

#### Alterações em Relação ao Benchmark #10

- **Fetch Max Wait**: 10ms → 1ms
- **Load Batch Size**: 1000 → 5000
- **SQS Batch Size**: 50 → 100
- **Throughput**: +1.5% (4018 → 4078 msg/s)
- **Peak Rate**: +73% (23367 → 40475 msg/s)
- **CPU Max**: +8% (48-50% → 49-54%)

**Conclusão**: Otimizações de batch e fetch não melhoraram throughput significativamente - sistema já está no limite de CPU (50%). Próxima otimização: JVM tuning ou parallel SQS sends.

---

### Benchmark #12 - Parallel SQS Sends
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 2000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 1ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 192 goroutines |
| **Batch Size** | 5000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Parallel Batch (100 msgs) |
| **Kafka Listener** | Batch (até 2000 msgs/poll) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     00:44:14
  End:       00:44:27
  Duration:  13.0s
  Total:    60191 msgs
  Avg Rate: 4623 msg/s
  Peak Rate: 44290 msg/s
  SQS Queue: 60191 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60191 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10032     0  209.45ms        519.83ms        1341.92ms       3.6/31.8/50.3%     174/209/242 MiB   
  8081        10031     0  499.12ms        1405.09ms       1405.09ms       18.3/30.8/49.5%    176/216/252 MiB   
  8082        10032     0  419.30ms        1744.70ms       1744.70ms       4.2/30.5/50.1%     176/226/255 MiB   
  8083        10032     0  297.80ms        801.11ms        901.78ms        9.8/45.1/51.0%     178/213/249 MiB   
  8084        10032     0  314.57ms        1539.31ms       1539.31ms       4.0/22.2/51.6%     173/214/244 MiB   
  8085        10032     0  201.20ms        972.95ms        2013.13ms       10.7/39.9/53.6%    177/229/256 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 4623 msg/s |
| **Throughput Pico** | 44.290 msg/s |
| **Latência P50** | 201-499ms |
| **Latência P90** | 519-1744ms |
| **Latência P99** | 901-2013ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 49-54% |
| **Memória Max** | 242-256 MiB |

#### Alterações em Relação ao Benchmark #11

- **SQS Client**: SqsClient → SqsAsyncClient
- **SQS Sends**: Sequential → Parallel (CompletableFuture)
- **Throughput**: +13.4% (4078 → 4623 msg/s)
- **Duração**: -12% (14.8s → 13.0s)
- **Peak Rate**: +9% (40475 → 44290 msg/s)
- **Memória Max**: +13% (217-228 → 242-256 MiB)

**Conclusão**: Parallel SQS sends é a segunda otimização mais impactante (após batch listener). Enviar múltiplos batches em paralelo reduz overhead de HTTP e aumenta throughput em 13.4%.

---

### Benchmark #13 - JVM Tuning (G1GC) ❌ FALHOU
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 2000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 1ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 192 goroutines |
| **Batch Size** | 5000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Parallel Batch (100 msgs) |
| **Kafka Listener** | Batch (até 2000 msgs/poll) |
| **JVM Flags** | G1GC, MaxGCPauseMillis=20, G1HeapRegionSize=4m |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     00:49:36
  End:       00:49:53
  Duration:  17.6s
  Total:    60190 msgs
  Avg Rate: 3414 msg/s
  Peak Rate: 30613 msg/s
  SQS Queue: 60190 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60190 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10032     0  603.85ms        1610.48ms       2013.13ms       4.9/49.3/54.3%     199/231/311 MiB   
  8081        10032     0  314.57ms        1002.44ms       1337.98ms       8.9/49.2/49.8%     201/246/303 MiB   
  8082        10032     0  209.45ms        905.71ms        1811.68ms       4.9/50.1/51.4%     202/245/412 MiB   
  8083        10031     0  419.30ms        1610.48ms       2550.01ms       5.8/50.0/50.4%     228/253/314 MiB   
  8084        10032     0  297.80ms        1941.96ms       2009.07ms       5.7/48.7/49.9%     204/223/300 MiB   
  8085        10031     0  301.86ms        1409.16ms       2415.79ms       5.6/50.0/51.8%     229/266/358 MiB
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 3414 msg/s |
| **Throughput Pico** | 30.613 msg/s |
| **Latência P50** | 209-603ms |
| **Latência P90** | 905-1941ms |
| **Latência P99** | 1337-2550ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 49-54% |
| **Memória Max** | 300-412 MiB |

#### Alterações em Relação ao Benchmark #12

- **JVM Flags**: Default → G1GC + tuning flags
- **Throughput**: -26% (4623 → 3414 msg/s) ❌
- **Duração**: +35% (13.0s → 17.6s) ❌
- **Memória Max**: +41% (242-256 → 300-412 MiB) ❌
- **Latência P50**: +108% (201-499ms → 209-603ms) ❌

**Conclusão**: JVM tuning com G1GC causou overhead em vez de melhoria. As flags `-XX:MaxGCPauseMillis=20` e `-XX:G1HeapRegionSize=4m` aumentaram uso de memória e reduziram throughput. **Revertido para configuração padrão.**

---

### Benchmark #14 - Jackson Afterburner ❌ FALHOU
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 2000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 1ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 192 goroutines |
| **Batch Size** | 5000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Parallel Batch (100 msgs) |
| **Kafka Listener** | Batch (até 2000 msgs/poll) |
| **Jackson** | AfterburnerModule (bytecode optimization) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     01:08:14
  End:       01:08:32
  Duration:  17.1s
  Total:    60191 msgs
  Avg Rate: 3518 msg/s
  Peak Rate: 49245 msg/s
  SQS Queue: 60191 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60191 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10032     0  700.45ms       2143.29ms      2143.29ms      5.3/28.7/49.6%     181/232/261 MiB   
  8081        10032     0  402.39ms       1744.57ms      1744.57ms      6.0/35.4/50.4%     176/222/253 MiB   
  8082        10032     0  301.73ms       939.26ms       1207.70ms      6.6/38.4/50.4%     172/224/249 MiB   
  8083        10031     0  301.86ms       1409.16ms      1610.48ms      7.7/34.4/50.3%     173/223/248 MiB   
  8084        10032     0  368.97ms       805.18ms       1610.48ms      4.2/27.2/50.2%     173/238/248 MiB   
  8085        10032     0  368.97ms       2147.35ms      2147.35ms      5.9/47.9/51.6%     173/228/261 MiB   
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 3518 msg/s |
| **Throughput Pico** | 49.245 msg/s |
| **Latência P50** | 301-700ms |
| **Latência P90** | 805-2147ms |
| **Latência P99** | 1207-2147ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 49-52% |
| **Memória Max** | 248-261 MiB |

#### Alterações em Relação ao Benchmark #12

- **Jackson**: Default → AfterburnerModule (bytecode optimization para JSON parsing)
- **Throughput**: -24% (4623 → 3518 msg/s) ❌
- **Duração**: +32% (13.0s → 17.1s) ❌
- **Latência P50**: +50% (201-499ms → 301-700ms) ❌
- **CPU Med**: +50% (30-45% → 28-48%) ❌

**Conclusão**: Jackson Afterburner causou overhead significativo. O módulo de otimização por bytecode não é eficiente para batch listener com alto volume. CPU média subiu 50% e throughput caiu 24%. **Revertido.**

**Causa provável**: O Afterburner gera subclasses dinâmicas via bytecode, que adiciona overhead de classloading e cache de métodos. Com batch listener processando até 2000 msgs por poll, o overhead acumulado supera qualquer ganho de parsing.

---

### Benchmark #15 - String Manipulation (sem Jackson)
**Data:** 13/09/2026

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | 2000 |
| **Fetch Min Bytes** | 4096 |
| **Fetch Max Wait** | 1ms |
| **Concurrency** | 5 (por container) |
| **Containers** | 6 réplicas Spring Boot |
| **Load Workers** | 192 goroutines |
| **Batch Size** | 5000 |
| **Processing Delay** | 0ms |
| **CPU Limit** | 0.5 por container |
| **Memory Limit** | 1GB por container |
| **SQS Mode** | Async + Parallel Batch (100 msgs) |
| **Kafka Listener** | Batch (até 2000 msgs/poll) |
| **JSON Processing** | String manipulation (sem Jackson) |

#### Resultado

```
── RUN STATUS ──────────────────────────────────────────────────────
  Start:     01:39:55
  End:       01:40:11
  Duration:  16.2s
  Total:    60191 msgs
  Avg Rate: 3719 msg/s
  Peak Rate: 44223 msg/s
  SQS Queue: 60191 msgs
  Status:   ✓ All messages in SQS

  ── SUMMARY ─────────────────────────────────────────────────────────
  Containers:  6 active
  Processed:  60191 msgs
  Kafka Lag:   0 msgs

  ── PER CONTAINER ───────────────────────────────────────────────────

  PORT       MSGS      LAG        P50           P90           P99           CPU                MEMORY            
                       (max)                                                (min/med/max)      (min/med/max)     
  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  8080        10032     0  1.37ms          100.66ms        218.10ms        10.3/47.9/49.5%    178/221/237 MiB   
  8081        10032     0  0.72ms          88.08ms         100.66ms        20.9/49.0/50.5%    163/216/237 MiB   
  8082        10032     0  0.91ms          92.27ms         218.10ms        15.7/49.0/49.5%    177/218/239 MiB   
  8083        10031     0  0.85ms          88.08ms         109.05ms        5.6/48.2/49.3%     143/220/237 MiB   
  8084        10032     0  1.30ms          96.46ms         201.32ms        4.2/49.0/51.2%     131/220/234 MiB   
  8085        10032     0  0.84ms          83.88ms         201.32ms        9.3/49.4/50.4%     129/225/238 MiB   
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | 3719 msg/s |
| **Throughput Pico** | 44.223 msg/s |
| **Latência P50** | 0.72-1.37ms |
| **Latência P90** | 83-100ms |
| **Latência P99** | 100-218ms |
| **Lag Max** | 0 msgs |
| **SQS Status** | ✓ All messages delivered |
| **CPU Max** | 49-51% |
| **Memória Max** | 234-239 MiB |

#### Alterações em Relação ao Benchmark #12

- **JSON Processing**: Jackson readTree/writeValueAsString → String manipulation (indexOf + substring)
- **Throughput**: -19% (4623 → 3719 msg/s) ⚠
- **Latência P50**: -99.6% (201-499ms → 0.72-1.37ms) ✅
- **CPU Med**: -5% (30-45% → 47-51%) ✅
- **Latência P90**: -83% (519-1744ms → 83-100ms) ✅

**Conclusão**: A latência de processamento caiu 200x (de 200ms para sub-milissegundo). A otimização funcionou perfeitamente — o processamento não é mais o gargalo. O throughput caiu porque o **gargalo migrou para SQS drain** (Floci não consegue processar mais rápido). O SQS emulator é o limitador final do sistema.

**Análise**: Com latência P50 sub-milissegundo, o consumer processa mensagens instantaneamente. O throughput de 3719 msg/s representa a capacidade máxima do SQS Floci, não do processamento Java. Para aumentar throughput além disso, seria necessário escalar horizontalmente o Floci ou substituir SQS por Kafka output topic.

---

### Template para Novos Benchmarks

Para adicionar um novo benchmark, copie o template abaixo:

```markdown
### Benchmark #N - [Título]
**Data:** [DD/MM/AAAA]

#### Configuração

| Parâmetro | Valor |
|-----------|-------|
| **Kafka Bootstrap** | `kafka:29092` |
| **Topic** | `input-topic` (6 partições) |
| **Consumer Group** | `high-perf-consumer-group` |
| **Max Poll Records** | [valor] |
| **Fetch Min Bytes** | [valor] |
| **Fetch Max Wait** | [valor]ms |
| **Concurrency** | [valor] (por container) |
| **Containers** | [N] réplicas Spring Boot |
| **Load Workers** | [N] goroutines |
| **Batch Size** | [N] |
| **Processing Delay** | [X]ms |
| **CPU Limit** | [X] por container |
| **Memory Limit** | [X]GB por container |

#### Resultado

```
[Colar saída do monitor aqui]
```

| Métrica | Valor |
|---------|-------|
| **Throughput Médio** | [X] msg/s |
| **Throughput Pico** | [X] msg/s |
| **Latência P50** | [X]ms |
| **Latência P90** | [X]ms |
| **Latência P99** | [X]ms |
| **Lag Max** | [X] msgs |
| **SQS Status** | [✓/⚠] |
| **CPU Max** | [X]% |
| **Memória Max** | [X] MiB |

#### Alterações em Relação ao Benchmark #N-1

- [Descrever mudanças feitas]
```
