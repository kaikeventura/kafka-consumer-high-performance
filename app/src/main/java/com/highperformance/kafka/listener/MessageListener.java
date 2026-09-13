package com.highperformance.kafka.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highperformance.kafka.service.SqsProducerService;
import io.micrometer.core.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {

    private static final Logger log = LoggerFactory.getLogger(MessageListener.class);

    private final SqsProducerService sqsProducerService;
    private final ObjectMapper objectMapper;

    @Value("${processing.delay.ms:0}")
    private long processingDelayMs;

    public MessageListener(SqsProducerService sqsProducerService, ObjectMapper objectMapper) {
        this.sqsProducerService = sqsProducerService;
        this.objectMapper = objectMapper;
    }

    @Timed(value = "kafka.listener.seconds", description = "Time spent processing Kafka messages")
    @KafkaListener(
            topics = "${spring.kafka.topic.input}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "1"
    )
    public void listen(String message) {
        try {
            if (processingDelayMs > 0) {
                Thread.sleep(processingDelayMs);
            }

            JsonNode jsonNode = objectMapper.readTree(message);

            String id = jsonNode.get("id").asText();
            String timestamp = jsonNode.get("timestamp").asText();

            String processedMessage = objectMapper.writeValueAsString(
                    new ProcessedMessage(id, timestamp, "processed")
            );

            sqsProducerService.sendMessage(processedMessage);

            log.debug("Message processed: id={}", id);
        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage(), e);
        }
    }

    record ProcessedMessage(String id, String timestamp, String status) {}
}
