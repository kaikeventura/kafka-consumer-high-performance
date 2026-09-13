package com.highperformance.kafka.service;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SqsProducerService {

    private static final Logger log = LoggerFactory.getLogger(SqsProducerService.class);

    private final SqsClient sqsClient;
    private final String queueUrl;
    private final int batchSize;
    private final ConcurrentLinkedQueue<String> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    public SqsProducerService(SqsClient sqsClient,
                              @Value("${sqs.queue-name}") String queueName,
                              @Value("${aws.endpoint-url}") String endpointUrl,
                              @Value("${sqs.batch-size:10}") int batchSize) {
        this.sqsClient = sqsClient;
        this.queueUrl = endpointUrl + "/000000000000/" + queueName;
        this.batchSize = batchSize;
    }

    @Async
    public void sendMessage(String messageBody) {
        buffer.offer(messageBody);
        pendingCount.incrementAndGet();

        if (buffer.size() >= batchSize) {
            flushBatch();
        }
    }

    @PreDestroy
    public void shutdown() {
        flushBatch();
    }

    private void flushBatch() {
        List<String> messages = new ArrayList<>();
        while (!buffer.isEmpty() && messages.size() < batchSize) {
            String msg = buffer.poll();
            if (msg != null) {
                messages.add(msg);
            }
        }

        if (messages.isEmpty()) {
            return;
        }

        try {
            List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                entries.add(SendMessageBatchRequestEntry.builder()
                        .id(String.valueOf(i))
                        .messageBody(messages.get(i))
                        .build());
            }

            SendMessageBatchRequest request = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            sqsClient.sendMessageBatch(request);
            pendingCount.addAndGet(-messages.size());
            log.debug("Batch sent to SQS: {} messages", messages.size());
        } catch (Exception e) {
            log.error("Error sending batch to SQS: {}", e.getMessage(), e);
            messages.forEach(buffer::offer);
            pendingCount.addAndGet(messages.size());
        }
    }

    public int getPendingCount() {
        return pendingCount.get();
    }
}
