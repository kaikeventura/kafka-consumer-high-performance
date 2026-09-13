package com.highperformance.kafka.listener;

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
    private final Counter messageCounter;

    public MessageListener(SqsProducerService sqsProducerService,
                           MeterRegistry meterRegistry) {
        this.sqsProducerService = sqsProducerService;
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
                String processedMessage = transformMessage(message);
                sqsProducerService.sendMessage(processedMessage);
                messageCounter.increment();
            } catch (Exception e) {
                log.error("Error processing message: {}", e.getMessage(), e);
            }
        }
    }

    private String transformMessage(String message) {
        int dataIdx = message.indexOf("\"data\":");
        if (dataIdx > 0) {
            int cutStart = dataIdx;
            if (message.charAt(dataIdx - 1) == ',') {
                cutStart--;
            }
            return message.substring(0, cutStart) + ",\"status\":\"processed\"}";
        }
        int lastBrace = message.lastIndexOf('}');
        return message.substring(0, lastBrace) + ",\"status\":\"processed\"}";
    }
}
