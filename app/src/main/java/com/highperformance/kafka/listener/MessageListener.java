package com.highperformance.kafka.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highperformance.kafka.service.SqsProducerService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MessageListener {

    private static final Logger log = LoggerFactory.getLogger(MessageListener.class);

    private final SqsProducerService sqsProducerService;
    private final ObjectMapper objectMapper;
    private final Counter messageCounter;

    public MessageListener(SqsProducerService sqsProducerService,
                           ObjectMapper objectMapper,
                           MeterRegistry meterRegistry) {
        this.sqsProducerService = sqsProducerService;
        this.objectMapper = objectMapper;
        this.messageCounter = Counter.builder("kafka.messages.processed")
                .description("Total messages processed")
                .register(meterRegistry);
    }

    @Timed(value = "kafka.listener.seconds", description = "Time spent processing Kafka messages")
    @KafkaListener(
            topics = "${spring.kafka.topic.input}",
            containerFactory = "batchFactory"
    )
    public void listenBatch(List<String> messages) {
        for (String message : messages) {
            try {
                JsonNode jsonNode = objectMapper.readTree(message);

                String id = jsonNode.get("id").asText();
                String timestamp = jsonNode.get("timestamp").asText();

                String processedMessage = objectMapper.writeValueAsString(
                        new ProcessedMessage(id, timestamp, "processed")
                );

                sqsProducerService.sendMessage(processedMessage);
                messageCounter.increment();

                log.debug("Message processed: id={}", id);
            } catch (Exception e) {
                log.error("Error processing message: {}", e.getMessage(), e);
            }
        }
    }

    record ProcessedMessage(String id, String timestamp, String status) {}
}
