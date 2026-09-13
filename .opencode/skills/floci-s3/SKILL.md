# 🛠️ Skill: Gerenciamento e Depuração de SQS (Floci)

**Descrição:** Habilidade para interagir, configurar e depurar filas do Amazon SQS localmente utilizando o emulador Floci. Inclui suporte para filas Standard, FIFO, Dead-Letter Queues (DLQ) e endpoints customizados de inspeção.

---

## 📍 1. Endpoints de Conexão

* **API Padrão SQS (AWS CLI/SDK):** `POST http://localhost:4566/`
* *Protocolos:* Query (XML) e JSON 1.0.
* *Formato da URL da Fila:* `http://localhost:4566/000000000000/<nome-da-fila>`


* **API de Inspeção Local (Exclusiva Floci):**
* `GET /_aws/sqs/messages?QueueUrl=<url>`: Lista todas as mensagens da fila (incluindo as em voo) **sem consumi-las** (não altera visibilidade ou contagem de recebimento).
* `DELETE /_aws/sqs/messages?QueueUrl=<url>`: Expurga (deleta) todas as mensagens da fila.



---

## ⚙️ 2. Ações Suportadas (AWS API)

O ambiente suporta as operações padrão do SQS, incluindo:

* **Gerenciamento de Filas:** `CreateQueue`, `DeleteQueue`, `ListQueues`, `GetQueueUrl`.
* **Mensageria:** `SendMessage`, `ReceiveMessage`, `DeleteMessage`.
* **Operações em Lote:** `SendMessageBatch`, `DeleteMessageBatch`, `ChangeMessageVisibilityBatch`.
* **Configuração e Tags:** `SetQueueAttributes`, `GetQueueAttributes`, `TagQueue`, `UntagQueue`.
* **Utilitários de Fila:** `PurgeQueue` (Deleta todas as mensagens).
* **DLQ (Dead Letter Queue):** `ListDeadLetterSourceQueues`, `StartMessageMoveTask`, `ListMessageMoveTasks`, `CancelMessageMoveTask`.

---

## 💻 3. Comandos Práticos (AWS CLI)

Certifique-se de exportar a variável de ambiente antes de executar os comandos:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

```

### Criar Filas

```bash
# Fila Standard
aws sqs create-queue --queue-name orders --endpoint-url $AWS_ENDPOINT_URL

# Fila FIFO
aws sqs create-queue --queue-name orders.fifo --attributes FifoQueue=true --endpoint-url $AWS_ENDPOINT_URL

```

### Ciclo de Vida da Mensagem

```bash
export QUEUE_URL="$AWS_ENDPOINT_URL/000000000000/orders"

# Enviar Mensagem
aws sqs send-message --queue-url $QUEUE_URL --message-body '{"event":"order.placed","id":"abc123"}' --endpoint-url $AWS_ENDPOINT_URL

# Receber Mensagem (Suporta Long Polling se configurado)
aws sqs receive-message --queue-url $QUEUE_URL --max-number-of-messages 10 --endpoint-url $AWS_ENDPOINT_URL

# Deletar Mensagem (Use o RECEIPT_HANDLE retornado no comando receive)
aws sqs delete-message --queue-url $QUEUE_URL --receipt-handle "RECEIPT_HANDLE" --endpoint-url $AWS_ENDPOINT_URL

```

### Inspeção de Mensagens (Sem Consumo)

```bash
# Espiar mensagens (Ideal para testes e debug)
curl "http://localhost:4566/_aws/sqs/messages?QueueUrl=$QUEUE_URL"

# Limpar fila inteira instantaneamente
curl -X DELETE "http://localhost:4566/_aws/sqs/messages?QueueUrl=$QUEUE_URL"

```

---

## 🔧 4. Long Polling e DLQ

**Long Polling:**
O `ReceiveMessage` pode aguardar de 0 a 20 segundos (`WaitTimeSeconds`). Se omitido, usa a configuração padrão da fila.

```bash
# Habilitar long polling de 20s para todos os consumidores da fila
aws sqs set-queue-attributes --queue-url $QUEUE_URL --attributes ReceiveMessageWaitTimeSeconds=20 --endpoint-url $AWS_ENDPOINT_URL

```

**Configurar Dead-Letter Queue (DLQ):**

```bash
# Assumindo que a orders-dlq já existe, primeiro obtemos o ARN dela:
DLQ_ARN=$(aws sqs get-queue-attributes --queue-url $AWS_ENDPOINT_URL/000000000000/orders-dlq --attribute-names QueueArn --query Attributes.QueueArn --output text --endpoint-url $AWS_ENDPOINT_URL)

# Vinculamos a DLQ à fila principal com o limite de 3 recebimentos
aws sqs set-queue-attributes --queue-url $QUEUE_URL --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":3}\"}" --endpoint-url $AWS_ENDPOINT_URL

```

---

## 🛠️ 5. Variáveis de Configuração do Sistema (Floci)

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `FLOCI_SERVICES_SQS_ENABLED` | `true` | Habilita ou desabilita o serviço SQS. |
| `FLOCI_SERVICES_SQS_DEFAULT_VISIBILITY_TIMEOUT` | `30` | Timeout padrão de visibilidade de mensagens (segundos). |
| `FLOCI_SERVICES_SQS_MAX_MESSAGE_SIZE` | `1048576` | Tamanho máximo da mensagem em bytes (1 MB). |
| `FLOCI_SERVICES_SQS_CLEAR_FIFO_DEDUPLICATION_CACHE_ON_PURGE` | `false` | Se `true`, limpar a fila (`PurgeQueue`) também limpa o cache de desduplicação FIFO da fila e tópicos SNS vinculados. |
